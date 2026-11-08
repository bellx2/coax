// Copyright 2026 Till0196
// SPDX-License-Identifier: Apache-2.0 OR GPL-2.0-or-later

package cli

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"syscall"

	"github.com/Till0196/coax/pkg/host"
	"github.com/Till0196/coax/pkg/mpegts/analyse"
)

// inputFlags は --input と一緒に使えるフラグ。ほかは選局とデーモンのためのもので、受信済みのストリームを読むときには効かない
var inputFlags = map[string]bool{
	"input": true, "output": true, "o": true,
	"stats": true, "program": true, "schedule": true, "verbose": true,
}

// tuneInput は coax が対応していないチューナーの出力など、受信済みのストリームを読んで書き出す。
// --program、--schedule と --stats は選局したときと同じ行を stderr に出す
func tuneInput(command *command, path, output string, stats, program, schedule bool, stdin io.Reader, stdout io.Writer) error {
	var refused []string
	command.flags.Visit(func(f *flag.Flag) {
		if !inputFlags[f.Name] {
			refused = append(refused, "--"+f.Name)
		}
	})
	if len(refused) > 0 {
		sort.Strings(refused)
		return fmt.Errorf("--input reads a stream that is already received, so it does not take %s", strings.Join(refused, ", "))
	}

	reader, closeReader, err := inputReader(path, stdin)
	if err != nil {
		return err
	}
	defer closeReader()
	writer, closeWriter, err := outputWriter(output, stdout)
	if err != nil {
		return err
	}
	defer closeWriter()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	if program || schedule {
		writer = io.MultiWriter(writer, newProgramWatcher(command.stderr, program, schedule))
	}
	if stats {
		counter := newInputCounter()
		writer = io.MultiWriter(writer, counter)
		go watchStats(ctx, counter.read, command.stderr)
	}

	// 標準入力の読み取りはシグナルでは止まらないので、コピーは別に回し、シグナルが来たらそのまま終わる
	done := make(chan error, 1)
	go func() {
		_, err := io.Copy(writer, reader)
		done <- err
	}()
	select {
	case <-ctx.Done():
		return nil
	case err := <-done:
		if err != nil {
			return fmt.Errorf("copy the input: %w", err)
		}
		return nil
	}
}

// inputCounter は入力を数え、デーモンがチューナーについて返すのと同じ形にする。
// ロックや CNR は入力からは分からないので空のままにする
type inputCounter struct {
	mu       sync.Mutex
	analyser *analyse.Analyser
}

func newInputCounter() *inputCounter {
	return &inputCounter{analyser: analyse.New()}
}

func (c *inputCounter) Write(p []byte) (int, error) {
	c.mu.Lock()
	c.analyser.Feed(p)
	c.mu.Unlock()
	return len(p), nil
}

func (c *inputCounter) read(context.Context) (host.Tuner, error) {
	c.mu.Lock()
	stats := c.analyser.Stats()
	c.mu.Unlock()
	counts := host.StreamCounts{
		Bytes:           stats.Bytes,
		Packets:         stats.Packets,
		Skipped:         stats.Skipped,
		SyncLosses:      stats.SyncLosses,
		Discontinuities: stats.Discontinuities,
		Nulls:           stats.Nulls,
		Errors:          stats.Errors,
		Scrambled:       stats.Scrambled,
	}
	// 何も届いていないうちは何も言えないので、ストリームの情報を付けない（--stats は行を出さない）
	if stats.Bytes == 0 {
		return host.Tuner{}, nil
	}
	// 届いたのに TS として揃わないときは、Format を空のままにして「not a multiplex」と出す
	if stats.Packets > 0 {
		counts.Format = "ts"
	}
	return host.Tuner{Stream: &counts}, nil
}
