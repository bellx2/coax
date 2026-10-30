// Copyright 2026 Till0196
// SPDX-License-Identifier: Apache-2.0 OR GPL-2.0-or-later

package cli

import (
	"bytes"
	"encoding/binary"
	"strings"
	"testing"
	"time"

	"github.com/Till0196/coax/pkg/isdb"
	"github.com/Till0196/coax/pkg/mpegts"
)

// eventSection は 1 番組だけを載せた、今と次の番組の表（EIT）のセクション
func eventSection(service uint16, number byte, eventID uint16, start time.Time, duration time.Duration, title []byte) []byte {
	epoch := time.Date(1858, time.November, 17, 0, 0, 0, 0, time.UTC)
	day := uint16(start.Sub(epoch).Hours() / 24)
	bcd := func(value int) byte { return byte(value/10<<4 | value%10) }
	short := append([]byte{'j', 'p', 'n', byte(len(title))}, title...)
	short = append(short, 0)
	descriptors := append([]byte{isdb.DescriptorTagShortEvent, byte(len(short))}, short...)

	event := binary.BigEndian.AppendUint16(nil, eventID)
	event = binary.BigEndian.AppendUint16(event, day)
	event = append(event, bcd(start.Hour()), bcd(start.Minute()), bcd(start.Second()))
	event = append(event, bcd(int(duration.Hours())), bcd(int(duration.Minutes())%60), 0)
	event = append(event, 0x80|byte(len(descriptors)>>8), byte(len(descriptors)))
	event = append(event, descriptors...)

	body := []byte{byte(service >> 8), byte(service), 0xc1, number, 1}
	body = append(body, 0x7f, 0xe0, 0x7f, 0xe0, 1, isdb.TableIDEITActual) // TSID、ONID、最後のセクションと表
	body = append(body, event...)
	length := len(body) + 4
	section := []byte{isdb.TableIDEITActual, 0xf0 | byte(length>>8), byte(length)}
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
	watcher := newProgramWatcher(&out)
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
