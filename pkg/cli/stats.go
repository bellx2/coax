// Copyright 2026 Till0196
// SPDX-License-Identifier: Apache-2.0 OR GPL-2.0-or-later

package cli

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/Till0196/coax/pkg/host"
)

// statsInterval is how often --stats reads the tuner: often enough to watch a
// figure move while turning an antenna, and slow enough for a rate taken over
// the interval to mean something.
const statsInterval = time.Second

// watchStats writes a line about the tuner every interval until ctx ends. The
// daemon's counters run from when the stream opened, so each line also says
// what changed since the one before: a total that has been growing all along
// hides whether it is still growing now.
//
// One line per reading rather than one rewritten in place, because the usual
// reader of the stream is a player writing to the same terminal.
func watchStats(ctx context.Context, client *host.Client, id string, w io.Writer) {
	ticker := time.NewTicker(statsInterval)
	defer ticker.Stop()
	var previous *host.Tuner
	last := time.Now()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			current, err := client.Tuner(ctx, id)
			if err != nil {
				if ctx.Err() == nil {
					fmt.Fprintf(w, "%s stats: %v\n", now.Format("15:04:05"), err)
				}
				continue
			}
			fmt.Fprintf(w, "%s %s\n", now.Format("15:04:05"), formatStats(previous, current, now.Sub(last)))
			previous, last = &current, now
		}
	}
}

// formatStats renders one reading. previous is nil for the first, which then
// has totals but no rate.
func formatStats(previous *host.Tuner, current host.Tuner, elapsed time.Duration) string {
	var parts []string
	switch {
	case current.Locked == nil:
	case *current.Locked:
		parts = append(parts, "locked")
	default:
		parts = append(parts, "unlocked")
	}
	if current.CNR != nil {
		parts = append(parts, fmt.Sprintf("CNR %.1fdB", *current.CNR))
	}
	if current.Strength != nil {
		parts = append(parts, fmt.Sprintf("%.0fdBm", *current.Strength))
	}
	if current.Quality != nil {
		parts = append(parts, fmt.Sprintf("quality %.0f%%", *current.Quality))
	}

	stream := current.Stream
	var before *host.StreamCounts
	if previous != nil {
		before = previous.Stream
	}
	if stream != nil && before != nil && elapsed > 0 {
		mbps := float64(stream.Bytes-before.Bytes) * 8 / elapsed.Seconds() / 1e6
		parts = append(parts, fmt.Sprintf("%.2f Mbps", mbps))
	}

	// 合計と前回からの増分を並べる。増えているかどうかが一目で分かるように
	counter := func(name string, now uint64, then *uint64) {
		if then == nil {
			parts = append(parts, fmt.Sprintf("%s %d", name, now))
			return
		}
		parts = append(parts, fmt.Sprintf("%s %d (+%d)", name, now, now-*then))
	}
	if current.Dropped != nil {
		var then *uint64
		if previous != nil {
			then = previous.Dropped
		}
		counter("dropped", *current.Dropped, then)
	}
	if stream == nil {
		return strings.Join(parts, "  ")
	}
	// 前回の値。最初の読み取りでは nil のままで、増分を出さない
	var was host.StreamCounts
	field := func(value *uint64) *uint64 {
		if before == nil {
			return nil
		}
		return value
	}
	if before != nil {
		was = *before
	}
	if stream.Format == "ts" {
		counter("errors", stream.Errors, field(&was.Errors))
	}
	counter("discontinuities", stream.Discontinuities, field(&was.Discontinuities))
	counter("sync losses", stream.SyncLosses, field(&was.SyncLosses))
	if stream.Format == "ts" && before != nil && stream.Packets > was.Packets {
		// 復号が要るかどうかの目安。スタッフィングは除いて数える
		payload := (stream.Packets - was.Packets) - (stream.Nulls - was.Nulls)
		if payload > 0 {
			parts = append(parts, fmt.Sprintf("scrambled %.0f%%",
				float64(stream.Scrambled-was.Scrambled)*100/float64(payload)))
		}
	}
	if stream.Format == "" {
		parts = append(parts, "not a multiplex")
	}
	return strings.Join(parts, "  ")
}
