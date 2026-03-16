package index

import (
	"context"
	"time"
)

type IndexStore interface {
	WriteGOP(ctx context.Context, cameraID string, meta GOPMeta) error
	QueryRange(ctx context.Context, cameraID string, start, end time.Time) ([]GOPMeta, error)
	WriteEvent(ctx context.Context, cameraID string, event RecordingEvent) error
	GetRecordingRanges(ctx context.Context, cameraID string, date time.Time) ([]TimeRange, error)
	Close() error
}

type GOPMeta struct {
	CameraID   string
	StartTime  time.Time
	EndTime    time.Time
	ObjectKey  string
	InitKey    string
	Size       int64
	FrameCount int
	Codec      string
}

type RecordingEvent struct {
	Type      string
	Timestamp time.Time
	Metadata  map[string]string
}

type TimeRange struct {
	Start time.Time
	End   time.Time
}
