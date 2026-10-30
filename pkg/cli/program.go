// Copyright 2026 Till0196
// SPDX-License-Identifier: Apache-2.0 OR GPL-2.0-or-later

package cli

import (
	"fmt"
	"io"
	"time"

	"github.com/Till0196/coax/pkg/isdb"
	"github.com/Till0196/coax/pkg/mpegts"
)

// 番組情報（EIT）が流れる PID。0x12 はフルセグ用（H-EIT）、0x26 と 0x27 はワンセグ用（M-EIT、L-EIT）
var eventPIDs = []uint16{0x12, 0x26, 0x27}

// programWatcher は書き出すストリームを横で読み、今の番組と次の番組が変わるたびに 1 行ずつ報告する。
// 書き込みは必ず成功させる。ストリームの書き出しを止めないため。
type programWatcher struct {
	w          io.Writer
	pending    []byte
	collectors map[uint16]*mpegts.SectionCollector
	// サービスごとの今の番組（[0]）と次の番組（[1]）
	events map[uint16]*[2]programEvent
	shown  [2]programEvent
}

type programEvent struct {
	id       uint16
	start    time.Time
	duration time.Duration
	// 開始と長さは不明のことがある
	startKnown, durationKnown bool
	title                     string
}

func newProgramWatcher(w io.Writer) *programWatcher {
	watcher := &programWatcher{
		w:          w,
		collectors: map[uint16]*mpegts.SectionCollector{},
		events:     map[uint16]*[2]programEvent{},
	}
	for _, pid := range eventPIDs {
		watcher.collectors[pid] = &mpegts.SectionCollector{}
	}
	return watcher
}

func (watcher *programWatcher) Write(data []byte) (int, error) {
	watcher.pending = append(watcher.pending, data...)
	// 次のパケットの先頭まで見えてから読む。同期バイトと同じ値はデータの中にもあるので、
	// 188 バイト先も同期バイトのときだけパケットの始まりとみなす
	for len(watcher.pending) >= 2*mpegts.PacketSize {
		// 同期バイトの位置に合わせ直す。読み出しの区切りはパケットの区切りと一致しない
		if watcher.pending[0] != mpegts.SyncByte || watcher.pending[mpegts.PacketSize] != mpegts.SyncByte {
			watcher.pending = watcher.pending[1:]
			continue
		}
		watcher.packet(watcher.pending[:mpegts.PacketSize])
		watcher.pending = watcher.pending[mpegts.PacketSize:]
	}
	// 溜めた分を詰め直す。append し続けると下の配列が大きくなり続ける
	watcher.pending = append(watcher.pending[:0:0], watcher.pending...)
	return len(data), nil
}

func (watcher *programWatcher) packet(packet []byte) {
	pid := uint16(packet[1]&0x1f)<<8 | uint16(packet[2])
	collector := watcher.collectors[pid]
	if collector == nil || packet[3]&0x10 == 0 {
		return
	}
	start := 4
	if packet[3]&0x20 != 0 {
		start += 1 + int(packet[4])
	}
	if start >= mpegts.PacketSize {
		return
	}
	collector.Push(packet[start:], packet[1]&0x40 != 0)
	for _, section := range collector.Take() {
		watcher.section(section)
	}
}

func (watcher *programWatcher) section(section []byte) {
	table := isdb.ParseEventTable(section)
	// 自分のストリームの、今と次の番組の表だけを見る。セクション 0 が今、1 が次
	if table == nil || table.TableID != isdb.TableIDEITActual || table.SectionNumber > 1 {
		return
	}
	pair := watcher.events[table.ServiceID]
	if pair == nil {
		pair = &[2]programEvent{}
		watcher.events[table.ServiceID] = pair
	}
	pair[table.SectionNumber] = programEvent{}
	if len(table.Events) > 0 {
		pair[table.SectionNumber] = eventOf(table.Events[0])
	}
	watcher.report()
}

func eventOf(event isdb.Event) programEvent {
	one := programEvent{
		id:            event.EventID,
		start:         event.Start,
		duration:      event.Duration,
		startKnown:    event.StartKnown,
		durationKnown: event.DurationKnown,
	}
	if descriptor, ok := isdb.FindDescriptor(event.Descriptors, isdb.DescriptorTagShortEvent); ok {
		if short, ok := isdb.ParseShortEvent(descriptor.Body); ok {
			one.title = short.Title
		}
	}
	return one
}

// report は表示するサービスの今と次のうち、変わったほうを書き出す。表示するのは、今の番組が分かっている中で
// サービス ID が一番小さいもの。ワンセグはフルセグと同じ番組を流すので、どちらでも同じ番組になる
func (watcher *programWatcher) report() {
	var chosen *[2]programEvent
	var chosenID uint16
	for id, pair := range watcher.events {
		if pair[0].title != "" && (chosen == nil || id < chosenID) {
			chosen, chosenID = pair, id
		}
	}
	if chosen == nil {
		return
	}
	now := time.Now().Format("15:04:05")
	for index, label := range []string{"now", "next"} {
		event := chosen[index]
		if event == watcher.shown[index] || event.title == "" {
			continue
		}
		watcher.shown[index] = event
		fmt.Fprintf(watcher.w, "%s %s %s %s\n", now, label, event.span(), event.title)
	}
}

// span は放送の時刻（日本時間）での「開始-終了」。分からないところは空にする
func (event programEvent) span() string {
	if !event.startKnown {
		return "-"
	}
	if !event.durationKnown {
		return event.start.Format("15:04") + "-"
	}
	return event.start.Format("15:04") + "-" + event.start.Add(event.duration).Format("15:04")
}
