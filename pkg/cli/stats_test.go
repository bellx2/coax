// Copyright 2026 Till0196
// SPDX-License-Identifier: Apache-2.0 OR GPL-2.0-or-later

package cli

import (
	"testing"
	"time"

	"github.com/Till0196/coax/pkg/host"
)

// TestFormatStats covers the first reading, which has nothing to subtract, and
// the ones after it, which say how much each counter moved.
func TestFormatStats(t *testing.T) {
	locked := true
	cnr, strength := 29.64, -52.2
	var dropped0, dropped1 uint64 = 0, 3
	first := host.Tuner{
		Locked: &locked, CNR: &cnr, Strength: &strength, Dropped: &dropped0,
		Stream: &host.StreamCounts{Format: "ts", Bytes: 1_000_000, Packets: 5000, Nulls: 1000, Errors: 2, Scrambled: 3000},
	}
	second := host.Tuner{
		Locked: &locked, CNR: &cnr, Strength: &strength, Dropped: &dropped1,
		Stream: &host.StreamCounts{Format: "ts", Bytes: 3_000_000, Packets: 15000, Nulls: 3000, Errors: 2, Discontinuities: 4, Scrambled: 7000},
	}
	cases := []struct {
		name     string
		previous *host.Tuner
		current  host.Tuner
		want     string
	}{
		{
			name:    "the first reading has totals only",
			current: first,
			want:    "locked  CNR 29.6dB  -52dBm  dropped 0  errors 2  discontinuities 0  sync losses 0",
		},
		{
			name:     "a later one adds the rate and what moved",
			previous: &first,
			current:  second,
			want:     "locked  CNR 29.6dB  -52dBm  16.00 Mbps  dropped 3 (+3)  errors 2 (+0)  discontinuities 4 (+4)  sync losses 0 (+0)  scrambled 50%",
		},
		{
			name:    "a tuner not yet streaming has only its signal",
			current: host.Tuner{Locked: new(bool)},
			want:    "unlocked",
		},
		{
			name:    "bytes that are not a multiplex say so",
			current: host.Tuner{Stream: &host.StreamCounts{SyncLosses: 7}},
			want:    "discontinuities 0  sync losses 7  not a multiplex",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := formatStats(c.previous, c.current, time.Second); got != c.want {
				t.Errorf("got  %q\nwant %q", got, c.want)
			}
		})
	}
}
