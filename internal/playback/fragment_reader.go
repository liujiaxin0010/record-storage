package playback

import (
	"context"
	"fmt"
	"time"

	"recording-server/internal/index"
	"recording-server/internal/recorder"
	"recording-server/internal/storage"
)

type FragmentReader struct {
	storage storage.StorageBackend
	index   index.IndexStore
}

func NewFragmentReader(store storage.StorageBackend, idx index.IndexStore) *FragmentReader {
	return &FragmentReader{storage: store, index: idx}
}

func (r *FragmentReader) ListGOPs(ctx context.Context, cameraID string, start time.Time, end *time.Time) ([]index.GOPMeta, error) {
	windowEnd := start.Add(24 * time.Hour)
	if end != nil {
		windowEnd = end.Add(2 * time.Second)
	}
	metas, err := r.index.QueryRange(ctx, cameraID, start.Add(-2*time.Second), windowEnd)
	if err != nil {
		return nil, err
	}
	return metas, nil
}

func (r *FragmentReader) LoadInit(ctx context.Context, key string) (recorder.InitSegment, error) {
	data, err := r.storage.Get(ctx, key)
	if err != nil {
		return recorder.InitSegment{}, err
	}
	return recorder.ParseInitSegment(data)
}

func (r *FragmentReader) ReadGOP(ctx context.Context, meta index.GOPMeta) ([]recorder.Sample, error) {
	data, err := r.storage.Get(ctx, meta.ObjectKey)
	if err != nil {
		return nil, err
	}
	samples, err := recorder.ParseFragment(data, recorder.Codec(meta.Codec), meta.StartTime)
	if err != nil {
		return nil, fmt.Errorf("parse fragment %s: %w", meta.ObjectKey, err)
	}
	return samples, nil
}
