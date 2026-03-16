package recorder

import (
	"testing"
	"time"
)

func TestBuildAndParseFragment(t *testing.T) {
	base := time.Date(2026, 3, 6, 12, 0, 0, 0, time.UTC)
	builder := NewFMP4Builder(CodecH264, CodecParameters{PayloadType: 96, SPS: []byte{0x67, 0x42, 0x00, 0x1f, 0xe5, 0x88, 0x80}, PPS: []byte{0x68, 0xce, 0x3c, 0x80}})
	initData, _, err := builder.BuildInit()
	if err != nil {
		t.Fatal(err)
	}
	if len(initData) < 8 || string(initData[4:8]) != "ftyp" {
		t.Fatalf("unexpected init prefix: %v", initData)
	}
	if _, err := ParseInitSegment(initData); err != nil {
		t.Fatalf("ParseInitSegment() error = %v", err)
	}
	fragment, err := builder.BuildFragment([]Sample{
		{Codec: CodecH264, Timestamp: base, Units: [][]byte{{0x67, 0x42, 0x00, 0x1f}, {0x68, 0xce, 0x3c, 0x80}, {0x65, 0x01}}, IsKey: true, FrameType: FrameTypeI},
		{Codec: CodecH264, Timestamp: base.Add(40 * time.Millisecond), Units: [][]byte{{0x41, 0x9a}}, IsKey: false, FrameType: FrameTypeP},
	}, base)
	if err != nil {
		t.Fatal(err)
	}
	if len(fragment) < 8 || string(fragment[4:8]) != "moof" {
		t.Fatalf("unexpected fragment prefix: %v", fragment[:8])
	}
	samples, err := ParseFragment(fragment, CodecH264, base)
	if err != nil {
		t.Fatal(err)
	}
	if len(samples) != 2 || len(samples[0].Units) != 3 || len(samples[1].Units) != 1 {
		t.Fatalf("unexpected parsed fragment: %+v", samples)
	}
	if !samples[0].IsKey || samples[1].IsKey {
		t.Fatalf("unexpected key frame flags: %+v", samples)
	}
}
