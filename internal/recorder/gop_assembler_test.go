package recorder

import (
	"testing"
	"time"
)

func TestGOPAssemblerSplitsOnNextKeyFrame(t *testing.T) {
	assembler := NewGOPAssembler()
	base := time.Date(2026, 3, 6, 12, 0, 0, 0, time.UTC)
	flush, err := assembler.AddSample(Sample{Codec: CodecH264, Timestamp: base, Units: [][]byte{{0x65}}, IsKey: true, FrameType: FrameTypeI})
	if err != nil || flush {
		t.Fatalf("unexpected add result: flush=%v err=%v", flush, err)
	}
	flush, err = assembler.AddSample(Sample{Codec: CodecH264, Timestamp: base.Add(time.Second), Units: [][]byte{{0x41}}, IsKey: false, FrameType: FrameTypeP})
	if err != nil || flush {
		t.Fatalf("unexpected second add result: flush=%v err=%v", flush, err)
	}
	flush, err = assembler.AddSample(Sample{Codec: CodecH264, Timestamp: base.Add(2 * time.Second), Units: [][]byte{{0x65}}, IsKey: true, FrameType: FrameTypeI})
	if err != nil || !flush {
		t.Fatalf("expected flush on next key frame, got flush=%v err=%v", flush, err)
	}
	items, _, err := assembler.Flush()
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 2 {
		t.Fatalf("expected 2 samples, got %d", len(items))
	}
}

func BenchmarkGOPAssembler(b *testing.B) {
	assembler := NewGOPAssembler()
	base := time.Date(2026, 3, 6, 12, 0, 0, 0, time.UTC)
	sample := Sample{
		Codec:     CodecH264,
		Timestamp: base,
		Units:     [][]byte{{0x65, 0x88, 0x84, 0x00}, {0x06, 0x05}},
		IsKey:     false,
		FrameType: FrameTypeP,
		Params: CodecParameters{
			Width:  1920,
			Height: 1080,
			SPS:    []byte{0x67, 0x64, 0x00, 0x28},
			PPS:    []byte{0x68, 0xee, 0x3c, 0x80},
		},
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = assembler.AddSample(sample)
		if i%30 == 0 {
			assembler.Flush()
		}
	}
}
