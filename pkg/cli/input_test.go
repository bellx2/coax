// Copyright 2026 Till0196
// SPDX-License-Identifier: Apache-2.0 OR GPL-2.0-or-later

package cli

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestTuneInputCopiesStdin は --input - で標準入力をそのまま書き出し、--program の行も出すことを確かめる
func TestTuneInputCopiesStdin(t *testing.T) {
	jst := time.FixedZone("JST", 9*60*60)
	start := time.Date(2026, 10, 30, 21, 0, 0, 0, jst)
	stream := append(packets(eventSection(1024, 0, 1, start, time.Hour, []byte("Now"))), nullPacket()...)

	var stdout, stderr bytes.Buffer
	if err := runTune([]string{"--input", "-", "--program"}, bytes.NewReader(stream), &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(stdout.Bytes(), stream) {
		t.Fatalf("stdout は入力と同じであるべき: got %d bytes, want %d", stdout.Len(), len(stream))
	}
	if !strings.Contains(stderr.String(), "now ") {
		t.Fatalf("--program の行が無い: %q", stderr.String())
	}
}

// TestTuneInputReportsSchedule は --input でも --schedule の行を出すことを確かめる
func TestTuneInputReportsSchedule(t *testing.T) {
	start := time.Date(2026, 11, 8, 23, 0, 0, 0, time.UTC)
	section := tableSection(0x50, 1, 1024, 0, 1, start, time.Hour, []byte("Next"), nil, nil)
	stream := append(packets(section), nullPacket()...)

	var stdout, stderr bytes.Buffer
	if err := runTune([]string{"--input", "-", "--schedule"}, bytes.NewReader(stream), &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(stderr.String(), " schedule {") {
		t.Fatalf("--schedule の行が無い: %q", stderr.String())
	}
}

// TestTuneInputReadsAFile はファイルを読んでファイルへ書き出す
func TestTuneInputReadsAFile(t *testing.T) {
	dir := t.TempDir()
	in, out := filepath.Join(dir, "in.ts"), filepath.Join(dir, "out.ts")
	stream := bytes.Repeat(nullPacket(), 10)
	if err := os.WriteFile(in, stream, 0o600); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	if err := runTune([]string{"--input", in, "-o", out}, nil, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, stream) {
		t.Fatalf("出力は入力と同じであるべき: got %d bytes, want %d", len(got), len(stream))
	}
}

// TestTuneInputRefusesTuning は選局やデーモンのためのフラグを --input と一緒に受けない
func TestTuneInputRefusesTuning(t *testing.T) {
	var stdout, stderr bytes.Buffer
	err := runTune([]string{"--input", "-", "--ch", "27", "--tuner", "x"}, bytes.NewReader(nil), &stdout, &stderr)
	if err == nil {
		t.Fatal("--ch と --tuner を受けてしまった")
	}
	if !strings.Contains(err.Error(), "--ch, --tuner") {
		t.Fatalf("拒んだフラグを名指しするべき: %v", err)
	}
}

// TestInputCounterMatchesTheDaemonShape は入力を数えた結果が --stats の行になることを確かめる
func TestInputCounterMatchesTheDaemonShape(t *testing.T) {
	counter := newInputCounter()
	before, _ := counter.read(context.Background())
	if line := formatStats(nil, before, time.Second); line != "" {
		t.Fatalf("何も届いていないうちは行を出さないべき: %q", line)
	}

	scrambled := nullPacket()
	scrambled[1], scrambled[2], scrambled[3] = 0x01, 0x00, 0x90 // PID 0x100、スクランブルあり
	_, _ = counter.Write(append(bytes.Repeat(nullPacket(), 4), scrambled...))
	current, _ := counter.read(context.Background())
	stream := current.Stream
	if stream.Format != "ts" || stream.Packets != 5 || stream.Nulls != 4 || stream.Scrambled != 1 {
		t.Fatalf("数え方が違う: %+v", *stream)
	}
	if current.Locked != nil || current.CNR != nil {
		t.Fatal("入力からはロックも CNR も分からない")
	}
	if line := formatStats(nil, current, time.Second); !strings.HasPrefix(line, "errors 0") {
		t.Fatalf("ロックや CNR の無い行になるべき: %q", line)
	}
}
