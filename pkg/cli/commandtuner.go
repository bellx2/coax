// Copyright 2026 Till0196
// SPDX-License-Identifier: Apache-2.0 OR GPL-2.0-or-later

package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	channelconfig "github.com/Till0196/coax/pkg/config"
	"github.com/Till0196/coax/pkg/tuner"
)

// commandTuner は coax が対応していないチューナーを、選局して TS を stdout に出し続ける外部コマンド
// （PIX-DT295 の pix-ts など）で動かす。チャンネルごとにコマンドを起動し、止めるときは SIGTERM を送る。
// ロックや信号の強さは分からないので、受信できるかはデータが届くかどうかで決まる
type commandTuner struct {
	// template は {ch}（地上波の物理チャンネル）を含むコマンドとその引数
	template []string
	// argv は Tune で {ch} を置き換えたもの
	argv   []string
	stream *commandStream
}

func newCommandTuner(command string) (*commandTuner, error) {
	words, err := splitCommand(command)
	if err != nil {
		return nil, fmt.Errorf("--tune-command: %w", err)
	}
	if len(words) == 0 {
		return nil, fmt.Errorf("--tune-command is empty")
	}
	if !strings.Contains(command, "{ch}") {
		return nil, fmt.Errorf("--tune-command has no {ch}, so every channel would run the same command")
	}
	return &commandTuner{template: words}, nil
}

func (t *commandTuner) Backend() string      { return "command" }
func (t *commandTuner) Name() string         { return filepath.Base(t.template[0]) }
func (t *commandTuner) RelativeStreams() int { return 0 }

// Supports は地上波だけ。{ch} は UHF の物理チャンネルの番号しか表せない
func (t *commandTuner) Supports(system string) (bool, error) {
	return channelconfig.NormalizeDeliverySystem(system) == "ISDBT", nil
}

func (t *commandTuner) Tune(properties map[string]string) error {
	t.closeStream()
	channel, ok := uhfChannel(properties["FREQUENCY"])
	if !ok || channelconfig.NormalizeDeliverySystem(properties["DELIVERY_SYSTEM"]) != "ISDBT" {
		return tuner.ErrOutOfRange
	}
	t.argv = make([]string, len(t.template))
	for i, word := range t.template {
		t.argv[i] = strings.ReplaceAll(word, "{ch}", strconv.Itoa(channel))
	}
	return nil
}

// Status は常にロックしているとみなす。外部コマンドからはロックを知る手段がないので、
// 受信できないチャンネルはデータが届かないこと（--data-timeout）で見分ける
func (t *commandTuner) Status() (tuner.Status, error) {
	return tuner.Status{Locked: true}, nil
}

func (t *commandTuner) OpenStream(bounds tuner.Bounds) (tuner.Stream, error) {
	if t.argv == nil {
		return nil, fmt.Errorf("nothing has been tuned")
	}
	t.closeStream()
	stream, err := startCommandStream(t.argv, bounds)
	if err != nil {
		return nil, err
	}
	t.stream = stream
	return stream, nil
}

func (t *commandTuner) Close() error {
	t.closeStream()
	return nil
}

func (t *commandTuner) closeStream() {
	if t.stream != nil {
		_ = t.stream.Close()
		t.stream = nil
	}
}

// uhfChannel は地上波の UHF の周波数（Hz）を物理チャンネルの番号にする。13ch の中心が 473.142857 MHz で 6 MHz おき
func uhfChannel(frequency string) (int, bool) {
	hz, err := strconv.ParseInt(strings.TrimSpace(frequency), 10, 64)
	if err != nil {
		return 0, false
	}
	const first, step = 473_142_857, 6_000_000
	offset := hz - first
	channel := 13 + int((offset+step/2)/step)
	// 中心から 1 MHz 以上ずれていれば UHF のチャンネルではない（ケーブルの周波数など）
	if offset < -step/2 || channel > 62 || abs64(offset-int64(channel-13)*step) > 1_000_000 {
		return 0, false
	}
	return channel, true
}

func abs64(v int64) int64 {
	if v < 0 {
		return -v
	}
	return v
}

// splitCommand はコマンドの文字列を語に分ける。空白で区切り、'…' と "…" の中の空白は区切らない
func splitCommand(command string) ([]string, error) {
	var words []string
	var word strings.Builder
	inWord := false
	var quote rune
	for _, r := range command {
		switch {
		case quote != 0:
			if r == quote {
				quote = 0
			} else {
				word.WriteRune(r)
			}
		case r == '\'' || r == '"':
			quote, inWord = r, true
		case r == ' ' || r == '\t' || r == '\n':
			if inWord {
				words = append(words, word.String())
				word.Reset()
				inWord = false
			}
		default:
			word.WriteRune(r)
			inWord = true
		}
	}
	if quote != 0 {
		return nil, fmt.Errorf("unterminated %c", quote)
	}
	if inWord {
		words = append(words, word.String())
	}
	return words, nil
}

// commandStream は外部コマンドの stdout。読み取りは別の goroutine で回し、Read は ctx の期限で戻れるようにする
type commandStream struct {
	cmd    *exec.Cmd
	chunks chan []byte
	done   chan struct{}
	exited chan struct{}
	bounds tuner.Bounds

	pending   []byte
	delivered int
	first     time.Time
}

func startCommandStream(argv []string, bounds tuner.Bounds) (*commandStream, error) {
	cmd := exec.Command(argv[0], argv[1:]...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start %s: %w", filepath.Base(argv[0]), err)
	}
	s := &commandStream{
		cmd: cmd, chunks: make(chan []byte, 64), done: make(chan struct{}), exited: make(chan struct{}),
		bounds: bounds,
	}
	go func() {
		defer close(s.chunks)
		buffer := make([]byte, 64*1024)
		for {
			n, err := stdout.Read(buffer)
			if n > 0 {
				select {
				case s.chunks <- append([]byte(nil), buffer[:n]...):
				case <-s.done:
					return
				}
			}
			if err != nil {
				return
			}
		}
	}()
	go func() {
		_ = cmd.Wait()
		close(s.exited)
	}()
	return s, nil
}

func (s *commandStream) Read(ctx context.Context, p []byte) (int, error) {
	if s.ended() {
		return 0, io.EOF
	}
	if len(s.pending) == 0 {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case chunk, ok := <-s.chunks:
			if !ok {
				return 0, io.EOF
			}
			s.pending = chunk
		}
	}
	if s.first.IsZero() {
		s.first = time.Now()
	}
	n := copy(p, s.pending)
	if s.bounds.Bytes > 0 && s.delivered+n > s.bounds.Bytes {
		n = s.bounds.Bytes - s.delivered
	}
	s.pending = s.pending[n:]
	s.delivered += n
	return n, nil
}

// ended は Bounds の分を渡し終えたか
func (s *commandStream) ended() bool {
	if s.bounds.Bytes > 0 && s.delivered >= s.bounds.Bytes {
		return true
	}
	return s.bounds.For > 0 && !s.first.IsZero() && time.Since(s.first) >= s.bounds.For
}

// Close は SIGTERM で止め、3 秒で終わらなければ止めきる（SIGTERM を送れない Windows では最初から）
func (s *commandStream) Close() error {
	select {
	case <-s.done:
		return nil
	default:
	}
	close(s.done)
	if err := s.cmd.Process.Signal(syscall.SIGTERM); err != nil && !errors.Is(err, os.ErrProcessDone) {
		_ = s.cmd.Process.Kill()
	}
	select {
	case <-s.exited:
	case <-time.After(3 * time.Second):
		_ = s.cmd.Process.Kill()
		<-s.exited
	}
	return nil
}
