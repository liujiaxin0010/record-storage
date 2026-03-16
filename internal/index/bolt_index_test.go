package index

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func TestBoltIndexQueryRangeAndRanges(t *testing.T) {
	idx, err := NewBoltIndex(filepath.Join(t.TempDir(), "recordings.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer idx.Close()

	base := time.Date(2026, 3, 6, 12, 0, 0, 0, time.UTC)
	for i := 0; i < 3; i++ {
		start := base.Add(time.Duration(i) * 2 * time.Second)
		end := start.Add(1500 * time.Millisecond)
		if err := idx.WriteGOP(context.Background(), "cam01", GOPMeta{CameraID: "cam01", StartTime: start, EndTime: end, ObjectKey: "obj", InitKey: "init", Codec: "h264"}); err != nil {
			t.Fatal(err)
		}
	}
	metas, err := idx.QueryRange(context.Background(), "cam01", base, base.Add(5*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if len(metas) != 3 {
		t.Fatalf("expected 3 metas, got %d", len(metas))
	}
	ranges, err := idx.GetRecordingRanges(context.Background(), "cam01", base)
	if err != nil {
		t.Fatal(err)
	}
	if len(ranges) != 1 {
		t.Fatalf("expected 1 range, got %d", len(ranges))
	}
}

func BenchmarkKeyEncoding(b *testing.B) {
	cameraID := "camera-001"
	ts := time.Now()

	b.Run("StringKey", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = makeGOPKey(cameraID, ts)
		}
	})

	b.Run("BinaryKey", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = MakeBinaryKey(cameraID, ts)
		}
	})
}

func BenchmarkIndexWrite(b *testing.B) {
	idx, _ := NewBoltIndex(filepath.Join(b.TempDir(), "bench.db"))
	defer idx.Close()

	meta := GOPMeta{
		CameraID:   "camera-001",
		StartTime:  time.Now(),
		EndTime:    time.Now().Add(2 * time.Second),
		ObjectKey:  "test/key",
		InitKey:    "test/init",
		Size:       1024,
		FrameCount: 30,
		Codec:      "h264",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		meta.StartTime = meta.StartTime.Add(time.Second)
		_ = idx.WriteGOP(context.Background(), meta.CameraID, meta)
	}
}
