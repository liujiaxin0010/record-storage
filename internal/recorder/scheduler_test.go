package recorder

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"recording-server/internal/config"
	"recording-server/internal/index"
	"recording-server/internal/storage"
)

func TestScheduleMatchesSameDayAndOvernight(t *testing.T) {
	sameDay := config.ScheduleConfig{Weekdays: []int{1}, Start: "08:00", End: "20:00"}
	if !scheduleMatches(sameDay, time.Date(2026, 3, 2, 9, 0, 0, 0, time.UTC)) {
		t.Fatal("expected same-day schedule to match")
	}
	if scheduleMatches(sameDay, time.Date(2026, 3, 2, 21, 0, 0, 0, time.UTC)) {
		t.Fatal("expected same-day schedule to not match")
	}
	overnight := config.ScheduleConfig{Weekdays: []int{1}, Start: "22:00", End: "02:00"}
	if !scheduleMatches(overnight, time.Date(2026, 3, 2, 23, 0, 0, 0, time.UTC)) {
		t.Fatal("expected overnight schedule to match same night")
	}
	if !scheduleMatches(overnight, time.Date(2026, 3, 3, 1, 0, 0, 0, time.UTC)) {
		t.Fatal("expected overnight schedule to match next morning")
	}
	if scheduleMatches(overnight, time.Date(2026, 3, 3, 3, 0, 0, 0, time.UTC)) {
		t.Fatal("expected overnight schedule to stop after end")
	}
}

func TestScheduleControllerStartsAndStopsRecorder(t *testing.T) {
	writer := newTestAsyncWriter(t)
	defer writer.Close()
	rec := NewStreamRecorder("cam01", writer)
	controller := NewScheduleController([]config.CameraConfig{{
		ID: "cam01",
		Recording: config.RecordingConfig{Schedule: []config.ScheduleConfig{{Weekdays: []int{1}, Start: "08:00", End: "20:00"}}},
	}}, map[string]*StreamRecorder{"cam01": rec})

	controller.Evaluate(time.Date(2026, 3, 2, 9, 0, 0, 0, time.UTC))
	if rec.State() != RecorderStateWaitingKeyFrame {
		t.Fatalf("expected waiting_key_frame, got %s", rec.State())
	}
	controller.Evaluate(time.Date(2026, 3, 2, 21, 0, 0, 0, time.UTC))
	if rec.State() != RecorderStateIdle {
		t.Fatalf("expected idle, got %s", rec.State())
	}
}

func TestStreamRecorderStateTransitions(t *testing.T) {
	writer := newTestAsyncWriter(t)
	defer writer.Close()
	rec := NewStreamRecorder("cam01", writer)
	rec.Start()
	if rec.State() != RecorderStateWaitingKeyFrame {
		t.Fatalf("expected waiting_key_frame, got %s", rec.State())
	}
	now := time.Date(2026, 3, 6, 12, 0, 0, 0, time.UTC)
	err := rec.ProcessSample(Sample{Codec: CodecH264, Timestamp: now, Units: [][]byte{{0x41}}, IsKey: false, FrameType: FrameTypeP, Params: CodecParameters{PayloadType: 96, SPS: []byte{0x67, 0x42, 0x00, 0x1f, 0xe5, 0x88, 0x80}, PPS: []byte{0x68, 0xce, 0x3c, 0x80}}})
	if err != nil {
		t.Fatal(err)
	}
	if rec.State() != RecorderStateWaitingKeyFrame {
		t.Fatalf("expected waiting_key_frame after non-key sample, got %s", rec.State())
	}
	err = rec.ProcessSample(Sample{Codec: CodecH264, Timestamp: now.Add(40 * time.Millisecond), Units: [][]byte{{0x65}}, IsKey: true, FrameType: FrameTypeI, Params: CodecParameters{PayloadType: 96, SPS: []byte{0x67, 0x42, 0x00, 0x1f, 0xe5, 0x88, 0x80}, PPS: []byte{0x68, 0xce, 0x3c, 0x80}}})
	if err != nil {
		t.Fatal(err)
	}
	if rec.State() != RecorderStateRecording {
		t.Fatalf("expected recording, got %s", rec.State())
	}
	rec.Stop()
	if rec.State() != RecorderStateIdle {
		t.Fatalf("expected idle after stop, got %s", rec.State())
	}
}

func TestAsyncWriterCloseDrainsQueuedJobs(t *testing.T) {
	store := &memoryStorage{objects: map[string][]byte{}}
	idx, err := index.NewBoltIndex(filepath.Join(t.TempDir(), "recordings.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer idx.Close()
	writer := NewAsyncWriter(store, idx, 1, 4)
	now := time.Date(2026, 3, 6, 12, 0, 0, 0, time.UTC)
	if err := writer.Submit(&WriteJob{CameraID: "cam01", GOPData: []byte("data"), InitData: []byte("init"), Meta: index.GOPMeta{CameraID: "cam01", StartTime: now, EndTime: now.Add(time.Second), ObjectKey: "a.frag", InitKey: "a.init", Codec: "h264"}}); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if _, ok := store.objects["a.frag"]; !ok {
		t.Fatal("expected fragment to be written before close")
	}
}

func newTestAsyncWriter(t *testing.T) *AsyncWriter {
	t.Helper()
	idx, err := index.NewBoltIndex(filepath.Join(t.TempDir(), "recordings.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = idx.Close() })
	return NewAsyncWriter(&memoryStorage{objects: map[string][]byte{}}, idx, 1, 8)
}

type memoryStorage struct{ objects map[string][]byte }

func (m *memoryStorage) Put(_ context.Context, key string, data []byte) error { m.objects[key] = append([]byte(nil), data...); return nil }
func (m *memoryStorage) Get(_ context.Context, key string) ([]byte, error) {
	data, ok := m.objects[key]
	if !ok { return nil, storage.ErrNotFound }
	return append([]byte(nil), data...), nil
}
func (m *memoryStorage) GetRange(_ context.Context, key string, offset, length int64) ([]byte, error) {
	data, err := m.Get(context.Background(), key)
	if err != nil { return nil, err }
	end := offset + length
	if end > int64(len(data)) { end = int64(len(data)) }
	return append([]byte(nil), data[offset:end]...), nil
}
func (m *memoryStorage) Delete(_ context.Context, key string) error { delete(m.objects, key); return nil }
func (m *memoryStorage) List(_ context.Context, prefix string) ([]string, error) { return nil, nil }
func (m *memoryStorage) Close() error { return nil }
