// Copyright 2026 Till0196
// SPDX-License-Identifier: Apache-2.0 OR GPL-2.0-or-later

package cli

import (
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"strings"
	"time"

	"github.com/Till0196/coax/pkg/isdb"
	"github.com/Till0196/coax/pkg/mpegts"
)

// 番組情報（EIT）が流れる PID。0x12 はフルセグ用（H-EIT）、0x26 と 0x27 はワンセグ用（M-EIT、L-EIT）
var eventPIDs = []uint16{0x12, 0x26, 0x27}

// programWatcher は書き出すストリームを横で読み、今の番組と次の番組が変わるたびに 1 行ずつ報告する（--program）。
// schedule なら、番組表（EIT のスケジュール）のセクションも 1 つ 1 行で報告する（--schedule）。
// 書き込みは必ず成功させる。ストリームの書き出しを止めないため。
type programWatcher struct {
	w          io.Writer
	pending    []byte
	collectors map[uint16]*mpegts.SectionCollector
	nowNext    bool
	schedule   bool
	// サービスごとの今の番組（[0]）と次の番組（[1]）
	events map[uint16]*[2]programEvent
	shown  [2]programEvent
	// 書いたスケジュールのセクションの版。同じ版は繰り返し届くので一度だけ書く
	scheduled map[scheduleSection]byte
}

// scheduleSection はスケジュールのセクションを見分けるもの
type scheduleSection struct {
	network, transport, service uint16
	table, number               byte
}

type programEvent struct {
	id       uint16
	start    time.Time
	duration time.Duration
	// 開始と長さは不明のことがある
	startKnown, durationKnown bool
	title                     string
	// 番組の説明（短形式イベント記述子の文）と、詳しい説明（拡張形式イベント記述子の項目）
	text  string
	items []isdb.ExtendedEventItem
}

func (event programEvent) equal(other programEvent) bool {
	return event.id == other.id && event.start == other.start && event.duration == other.duration &&
		event.startKnown == other.startKnown && event.durationKnown == other.durationKnown &&
		event.title == other.title && event.text == other.text && slices.Equal(event.items, other.items)
}

func newProgramWatcher(w io.Writer, nowNext, schedule bool) *programWatcher {
	watcher := &programWatcher{
		w:          w,
		collectors: map[uint16]*mpegts.SectionCollector{},
		nowNext:    nowNext,
		schedule:   schedule,
		events:     map[uint16]*[2]programEvent{},
		scheduled:  map[scheduleSection]byte{},
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
	if table == nil {
		return
	}
	if table.Schedule() {
		if watcher.schedule {
			watcher.reportSchedule(table)
		}
		return
	}
	// 自分のストリームの、今と次の番組の表だけを見る。セクション 0 が今、1 が次
	if !watcher.nowNext || table.TableID != isdb.TableIDEITActual || table.SectionNumber > 1 {
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
	var extended [][]byte
	for _, descriptor := range event.Descriptors {
		switch descriptor.Tag {
		case isdb.DescriptorTagShortEvent:
			if short, ok := isdb.ParseShortEvent(descriptor.Body); ok {
				one.title, one.text = short.Title, short.Text
			}
		case isdb.DescriptorTagExtendedEvent:
			extended = append(extended, descriptor.Body)
		}
	}
	// 項目は複数の記述子にまたがることがあるので、まとめてから読む
	if len(extended) > 0 {
		one.items = isdb.JoinExtendedEvents(extended)
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
		if event.equal(watcher.shown[index]) || event.title == "" {
			continue
		}
		watcher.shown[index] = event
		// 番組の行のあとに、説明を「-text」、詳しい説明の項目を「-item」の行で続ける。どちらも無ければ書かない
		fmt.Fprintf(watcher.w, "%s %s %s %s\n", now, label, event.span(), event.title)
		if event.text != "" {
			fmt.Fprintf(watcher.w, "%s %s-text %s\n", now, label, oneLine(event.text))
		}
		for _, item := range event.items {
			fmt.Fprintf(watcher.w, "%s %s-item %s\t%s\n", now, label, oneLine(item.Heading), oneLine(item.Value))
		}
	}
}

// 1 行に収まるよう、改行とタブを「\n」「\t」と書き、「\」は「\\」と書く。
// 8 単位符号の改行（APR）は CR になるので、CR も改行として扱う
var lineEscaper = strings.NewReplacer("\\", "\\\\", "\r\n", "\\n", "\r", "\\n", "\n", "\\n", "\t", "\\t")

func oneLine(text string) string {
	return lineEscaper.Replace(text)
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

// scheduleLine は --schedule の 1 行の JSON。1 行が EIT のスケジュールの 1 セクション
type scheduleLine struct {
	Network   uint16          `json:"network"`
	Transport uint16          `json:"transport"`
	Service   uint16          `json:"service"`
	Table     byte            `json:"table"`
	Section   byte            `json:"section"`
	Version   byte            `json:"version"`
	Events    []scheduleEvent `json:"events"`
}

type scheduleEvent struct {
	ID uint16 `json:"id"`
	// 放送の時刻（日本時間）。分からなければ省く
	Start    string                   `json:"start,omitempty"`
	Duration *int64                   `json:"duration,omitempty"`
	Title    string                   `json:"title,omitempty"`
	Text     string                   `json:"text,omitempty"`
	Items    []isdb.ExtendedEventItem `json:"items,omitempty"`
}

// reportSchedule はスケジュールのセクションを、版が変わったときだけ 1 行の JSON で書く。
// 番組の無いセクションも書く。その区間の番組が無くなったことを伝えるため
func (watcher *programWatcher) reportSchedule(table *isdb.EventTable) {
	key := scheduleSection{
		network: table.OriginalNetworkID, transport: table.TransportStreamID, service: table.ServiceID,
		table: table.TableID, number: table.SectionNumber,
	}
	if version, ok := watcher.scheduled[key]; ok && version == table.Version {
		return
	}
	watcher.scheduled[key] = table.Version
	line := scheduleLine{
		Network: key.network, Transport: key.transport, Service: key.service,
		Table: key.table, Section: key.number, Version: table.Version,
		Events: []scheduleEvent{},
	}
	for _, event := range table.Events {
		one := eventOf(event)
		out := scheduleEvent{ID: one.id, Title: one.title, Text: one.text, Items: one.items}
		if one.startKnown {
			out.Start = one.start.Format("2006-01-02T15:04:05")
		}
		if one.durationKnown {
			seconds := int64(one.duration / time.Second)
			out.Duration = &seconds
		}
		line.Events = append(line.Events, out)
	}
	// 番組名の「&」などをそのまま書く。Encode は最後に改行を足す
	var encoded strings.Builder
	encoder := json.NewEncoder(&encoded)
	encoder.SetEscapeHTML(false)
	if encoder.Encode(line) != nil {
		return
	}
	fmt.Fprintf(watcher.w, "%s schedule %s", time.Now().Format("15:04:05"), encoded.String())
}
