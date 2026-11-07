// Copyright 2026 Till0196
// SPDX-License-Identifier: Apache-2.0 OR GPL-2.0-or-later

package cli

import (
	"context"
	"errors"
	"io"
	"reflect"
	"runtime"
	"testing"
	"time"

	"github.com/Till0196/coax/pkg/tuner"
)

func TestUHFChannel(t *testing.T) {
	cases := []struct {
		frequency string
		want      int
		ok        bool
	}{
		{"473142857", 13, true},
		{"557142857", 27, true},
		{"767142857", 62, true},
		{"773142857", 0, false}, // 63ch は無い
		{"467142857", 0, false}, // 12ch 相当
		{"111000000", 0, false}, // ケーブルの周波数
		{"x", 0, false},
	}
	for _, c := range cases {
		got, ok := uhfChannel(c.frequency)
		if got != c.want || ok != c.ok {
			t.Errorf("uhfChannel(%s) = %d, %v; want %d, %v", c.frequency, got, ok, c.want, c.ok)
		}
	}
}

func TestSplitCommand(t *testing.T) {
	got, err := splitCommand(`"/Applications/pix userland/pix-ts" --receiver 0 -c 'T{ch}'  -o -`)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"/Applications/pix userland/pix-ts", "--receiver", "0", "-c", "T{ch}", "-o", "-"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %q, want %q", got, want)
	}
	if _, err := splitCommand(`pix-ts "T{ch}`); err == nil {
		t.Fatal("閉じていない引用符を受けてしまった")
	}
}

func TestCommandTunerNeedsAChannel(t *testing.T) {
	if _, err := newCommandTuner("pix-ts -c T27"); err == nil {
		t.Fatal("{ch} の無いコマンドを受けてしまった")
	}
}

// TestCommandTunerTunesAndStreams は {ch} を置き換えて起動し、Bounds の分だけ読んで止められることを確かめる
func TestCommandTunerTunesAndStreams(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("sh で偽のコマンドを動かす")
	}
	device, err := newCommandTuner(`sh -c 'printf "ch={ch}"; exec sleep 30'`)
	if err != nil {
		t.Fatal(err)
	}
	if ok, _ := device.Supports("ISDB-T"); !ok {
		t.Fatal("地上波を受けるべき")
	}
	if ok, _ := device.Supports("ISDBS"); ok {
		t.Fatal("衛星は受けないべき")
	}
	if err := device.Tune(map[string]string{"DELIVERY_SYSTEM": "ISDBT", "FREQUENCY": "111000000"}); !errors.Is(err, tuner.ErrOutOfRange) {
		t.Fatalf("UHF でない周波数は範囲外であるべき: %v", err)
	}
	if err := device.Tune(map[string]string{"DELIVERY_SYSTEM": "ISDBT", "FREQUENCY": "557142857"}); err != nil {
		t.Fatal(err)
	}
	stream, err := device.OpenStream(tuner.Bounds{Bytes: 5})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	buffer := make([]byte, 64)
	n, err := stream.Read(ctx, buffer)
	if err != nil || string(buffer[:n]) != "ch=27" {
		t.Fatalf("got %q, %v; want \"ch=27\"", buffer[:n], err)
	}
	if _, err := stream.Read(ctx, buffer); !errors.Is(err, io.EOF) {
		t.Fatalf("Bounds の分を読んだら終わるべき: %v", err)
	}
	// 何も出さなくなったコマンドを読むと、期限で戻る
	short, cancelShort := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancelShort()
	stream2, err := device.OpenStream(tuner.Bounds{})
	if err != nil {
		t.Fatal(err)
	}
	_, _ = stream2.Read(ctx, buffer)
	if _, err := stream2.Read(short, buffer); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("データが来ないときは期限で戻るべき: %v", err)
	}
	started := time.Now()
	if err := device.Close(); err != nil {
		t.Fatal(err)
	}
	if time.Since(started) > 2*time.Second {
		t.Fatal("SIGTERM ですぐ止まるべき")
	}
}
