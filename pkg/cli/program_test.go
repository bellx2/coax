// Copyright 2026 Till0196
// SPDX-License-Identifier: Apache-2.0 OR GPL-2.0-or-later

package cli

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/Till0196/coax/pkg/isdb"
	"github.com/Till0196/coax/pkg/mpegts"
)

// eventSection は 1 番組だけを載せた、今と次の番組の表（EIT）のセクション
func eventSection(service uint16, number byte, eventID uint16, start time.Time, duration time.Duration, title []byte) []byte {
	return detailedEventSection(service, number, eventID, start, duration, title, nil, nil)
}

// detailedEventSection は説明の文（text）と、拡張形式イベント記述子の中身（extended、1 つが 1 記述子）も載せる
func detailedEventSection(service uint16, number byte, eventID uint16, start time.Time, duration time.Duration,
	title, text []byte, extended [][]byte) []byte {
	return tableSection(isdb.TableIDEITActual, 1, service, number, eventID, start, duration, title, text, extended)
}

// tableSection は表（table）と版（version）も指定する。スケジュールの表（0x50 から）にも使う
func tableSection(table, version byte, service uint16, number byte, eventID uint16, start time.Time, duration time.Duration,
	title, text []byte, extended [][]byte) []byte {
	epoch := time.Date(1858, time.November, 17, 0, 0, 0, 0, time.UTC)
	day := uint16(start.Sub(epoch).Hours() / 24)
	bcd := func(value int) byte { return byte(value/10<<4 | value%10) }
	short := append([]byte{'j', 'p', 'n', byte(len(title))}, title...)
	short = append(short, byte(len(text)))
	short = append(short, text...)
	descriptors := append([]byte{isdb.DescriptorTagShortEvent, byte(len(short))}, short...)
	for _, body := range extended {
		descriptors = append(descriptors, isdb.DescriptorTagExtendedEvent, byte(len(body)))
		descriptors = append(descriptors, body...)
	}

	event := binary.BigEndian.AppendUint16(nil, eventID)
	event = binary.BigEndian.AppendUint16(event, day)
	event = append(event, bcd(start.Hour()), bcd(start.Minute()), bcd(start.Second()))
	event = append(event, bcd(int(duration.Hours())), bcd(int(duration.Minutes())%60), 0)
	event = append(event, 0x80|byte(len(descriptors)>>8), byte(len(descriptors)))
	event = append(event, descriptors...)

	body := []byte{byte(service >> 8), byte(service), 0xc1 | version<<1, number, 1}
	body = append(body, 0x7f, 0xe0, 0x7f, 0xe0, 1, table) // TSID、ONID、最後のセクションと表
	body = append(body, event...)
	length := len(body) + 4
	section := []byte{table, 0xf0 | byte(length>>8), byte(length)}
	section = append(section, body...)
	return binary.BigEndian.AppendUint32(section, mpegts.CRC32(section))
}

// packets はセクションを PID 0x27（ワンセグの番組情報）のパケットに分ける
func packets(section []byte) []byte {
	payload := append([]byte{0}, section...)
	var out []byte
	for first := true; len(payload) > 0; first = false {
		packet := bytes.Repeat([]byte{0xff}, mpegts.PacketSize)
		packet[0], packet[1], packet[2], packet[3] = mpegts.SyncByte, 0x00, 0x27, 0x10
		if first {
			packet[1] |= 0x40
		}
		payload = payload[copy(packet[4:], payload):]
		out = append(out, packet...)
	}
	return out
}

// nullPacket は空のパケット。ウォッチャーは次のパケットの先頭が見えてから読むので、最後に足す
func nullPacket() []byte {
	packet := bytes.Repeat([]byte{0xff}, mpegts.PacketSize)
	packet[0], packet[1], packet[2], packet[3] = mpegts.SyncByte, 0x1f, 0xff, 0x10
	return packet
}

func TestProgramWatcherReportsNowAndNext(t *testing.T) {
	day := time.Date(2026, 10, 30, 0, 0, 0, 0, time.UTC)
	// ARIB の 8 単位符号として読むので 2 バイトで漢字 1 文字になる。中身は問わない
	news, doc := []byte("NEWS"), []byte("TALK")
	stream := []byte{0x00, 0x47, 0x01} // 区切りがずれていても同期し直す
	stream = append(stream, packets(eventSection(1408, 0, 1, day.Add(21*time.Hour), time.Hour, news))...)
	stream = append(stream, packets(eventSection(1408, 1, 2, day.Add(22*time.Hour), 30*time.Minute, doc))...)
	stream = append(stream, nullPacket()...)

	var out bytes.Buffer
	watcher := newProgramWatcher(&out, true, false)
	// 読み出しの区切りはパケットの区切りと一致しない
	for len(stream) > 0 {
		n := min(100, len(stream))
		if written, err := watcher.Write(stream[:n]); written != n || err != nil {
			t.Fatalf("Write = %d, %v", written, err)
		}
		stream = stream[n:]
	}

	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	last := lines[len(lines)-2:]
	wantNow := "now 21:00-22:00 " + isdb.DescriptorText(news)
	wantNext := "next 22:00-22:30 " + isdb.DescriptorText(doc)
	if !strings.HasSuffix(last[0], wantNow) || !strings.HasSuffix(last[1], wantNext) {
		t.Fatalf("got %q, want lines ending %q and %q", lines, wantNow, wantNext)
	}

	// 変わらなければ何も書かない
	out.Reset()
	watcher.Write(append(packets(eventSection(1408, 0, 1, day.Add(21*time.Hour), time.Hour, news)), nullPacket()...))
	if out.Len() != 0 {
		t.Errorf("wrote %q for a table that had not changed", out.String())
	}
}

// extendedBody は拡張形式イベント記述子の中身。items は見出しと値を交互に並べる
func extendedBody(number, last byte, items ...[]byte) []byte {
	var list []byte
	for _, item := range items {
		list = append(list, byte(len(item)))
		list = append(list, item...)
	}
	body := []byte{number<<4 | last, 'j', 'p', 'n', byte(len(list))}
	body = append(body, list...)
	return append(body, 0) // text_length
}

func TestProgramWatcherReportsDetails(t *testing.T) {
	day := time.Date(2026, 10, 30, 0, 0, 0, 0, time.UTC)
	// 8 単位符号の 0x0D（APR）は改行になる。値は 2 つの記述子にまたがり、見出しの無い 2 つ目が続き
	title, text := []byte("NEWS"), []byte("AB\rCD")
	cast, castValue, rest := []byte("EF"), []byte("GH"), []byte("IJ")
	section := detailedEventSection(1408, 0, 1, day.Add(21*time.Hour), time.Hour, title, text, [][]byte{
		extendedBody(0, 1, cast, castValue),
		extendedBody(1, 1, nil, rest),
	})
	var out bytes.Buffer
	watcher := newProgramWatcher(&out, true, false)
	watcher.Write(append(packets(section), nullPacket()...))

	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	want := []string{
		"now 21:00-22:00 " + isdb.DescriptorText(title),
		"now-text " + strings.ReplaceAll(isdb.DescriptorText(text), "\r", `\n`),
		"now-item " + isdb.DescriptorText(cast) + "\t" + isdb.DescriptorText(append(append([]byte{}, castValue...), rest...)),
	}
	if len(lines) != len(want) {
		t.Fatalf("got %q, want %d lines", lines, len(want))
	}
	for i := range want {
		if !strings.HasSuffix(lines[i], want[i]) {
			t.Errorf("line %d = %q, want suffix %q", i, lines[i], want[i])
		}
	}
	if !strings.Contains(lines[1], `\n`) {
		t.Errorf("text line %q should carry the line break as \\n", lines[1])
	}
}

func TestProgramWatcherReportsSchedule(t *testing.T) {
	day := time.Date(2026, 11, 8, 0, 0, 0, 0, time.UTC)
	title := []byte("NEWS")
	now := tableSection(isdb.TableIDEITActual, 1, 1408, 0, 1, day.Add(21*time.Hour), time.Hour, title, nil, nil)
	scheduled := func(version byte) []byte {
		return tableSection(isdb.TableIDEITScheduleLow, version, 1408, 8, 7, day.Add(23*time.Hour), 30*time.Minute, title, nil, nil)
	}

	var out bytes.Buffer
	watcher := newProgramWatcher(&out, false, true)
	watcher.Write(append(append(packets(now), packets(scheduled(1))...), nullPacket()...))
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	// --program を付けなければ今の番組は書かない
	if len(lines) != 1 {
		t.Fatalf("got %q, want one schedule line", lines)
	}
	_, encoded, ok := strings.Cut(lines[0], " schedule ")
	if !ok {
		t.Fatalf("line %q is not a schedule line", lines[0])
	}
	var got scheduleLine
	if err := json.Unmarshal([]byte(encoded), &got); err != nil {
		t.Fatalf("unmarshal %q: %v", encoded, err)
	}
	if got.Service != 1408 || got.Network != 0x7fe0 || got.Table != isdb.TableIDEITScheduleLow || got.Section != 8 ||
		got.Version != 1 || len(got.Events) != 1 {
		t.Fatalf("got %+v", got)
	}
	event := got.Events[0]
	if event.ID != 7 || event.Start != "2026-11-08T23:00:00" || event.Duration == nil || *event.Duration != 1800 ||
		event.Title != isdb.DescriptorText(title) {
		t.Errorf("got event %+v", event)
	}

	// 同じ版は書かず、版が変われば書く
	out.Reset()
	watcher.Write(append(packets(scheduled(1)), nullPacket()...))
	if out.Len() != 0 {
		t.Errorf("wrote %q for a section already written", out.String())
	}
	watcher.Write(append(packets(scheduled(2)), nullPacket()...))
	if !strings.Contains(out.String(), `"version":2`) {
		t.Errorf("got %q, want the new version written", out.String())
	}

	// --program だけならスケジュールは書かない
	out.Reset()
	only := newProgramWatcher(&out, true, false)
	only.Write(append(packets(scheduled(1)), nullPacket()...))
	if out.Len() != 0 {
		t.Errorf("wrote %q without --schedule", out.String())
	}
}
