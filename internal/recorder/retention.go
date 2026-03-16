package recorder

import (
	"context"
	"log"
	"sync"
	"time"

	"recording-server/internal/index"
	"recording-server/internal/storage"
)

// RetentionCleaner periodically scans the index for expired recordings
// and deletes them from both storage and index.
type RetentionCleaner struct {
	storage       storage.StorageBackend
	index         index.IndexStore
	retentionDays int
	cameraIDs     []string
	interval      time.Duration

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func NewRetentionCleaner(store storage.StorageBackend, idx index.IndexStore, retentionDays int, cameraIDs []string) *RetentionCleaner {
	ctx, cancel := context.WithCancel(context.Background())
	return &RetentionCleaner{
		storage:       store,
		index:         idx,
		retentionDays: retentionDays,
		cameraIDs:     cameraIDs,
		interval:      1 * time.Hour,
		ctx:           ctx,
		cancel:        cancel,
	}
}

func (r *RetentionCleaner) Start() {
	r.wg.Add(1)
	go r.run()
	log.Printf("retention cleaner started: retention=%d days, interval=%v", r.retentionDays, r.interval)
}

func (r *RetentionCleaner) Stop() {
	r.cancel()
	r.wg.Wait()
}

func (r *RetentionCleaner) run() {
	defer r.wg.Done()
	// Run once at startup after a short delay
	select {
	case <-r.ctx.Done():
		return
	case <-time.After(30 * time.Second):
		r.cleanup()
	}

	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		select {
		case <-r.ctx.Done():
			return
		case <-ticker.C:
			r.cleanup()
		}
	}
}

func (r *RetentionCleaner) cleanup() {
	cutoff := time.Now().AddDate(0, 0, -r.retentionDays)
	log.Printf("retention cleanup: deleting recordings before %s", cutoff.Format("2006-01-02"))

	for _, cameraID := range r.cameraIDs {
		r.cleanupCamera(cameraID, cutoff)
	}
}

func (r *RetentionCleaner) cleanupCamera(cameraID string, cutoff time.Time) {
	// Scan day by day from the oldest possible date up to the cutoff
	// We scan the last retentionDays+30 days back to catch any stragglers
	scanStart := cutoff.AddDate(0, 0, -30)
	ctx := r.ctx

	for d := scanStart; d.Before(cutoff); d = d.Add(24 * time.Hour) {
		if ctx.Err() != nil {
			return
		}

		dayStart := time.Date(d.Year(), d.Month(), d.Day(), 0, 0, 0, 0, time.UTC)
		dayEnd := dayStart.Add(24 * time.Hour)

		gops, err := r.index.QueryRange(ctx, cameraID, dayStart, dayEnd)
		if err != nil {
			log.Printf("retention: failed to query %s/%s: %v", cameraID, d.Format("2006-01-02"), err)
			continue
		}
		if len(gops) == 0 {
			continue
		}

		deleted := 0
		for _, gop := range gops {
			if gop.EndTime.After(cutoff) {
				continue
			}
			// Delete data object from storage
			if err := r.storage.Delete(ctx, gop.ObjectKey); err != nil {
				log.Printf("retention: failed to delete object %s: %v", gop.ObjectKey, err)
				continue
			}
			// Delete init segment if present (best effort, may be shared)
			if gop.InitKey != "" {
				_ = r.storage.Delete(ctx, gop.InitKey)
			}
			deleted++
		}

		// Delete expired GOPs from the index
		if err := r.deleteExpiredFromIndex(ctx, cameraID, dayStart, dayEnd, cutoff); err != nil {
			log.Printf("retention: failed to clean index %s/%s: %v", cameraID, d.Format("2006-01-02"), err)
		}

		if deleted > 0 {
			log.Printf("retention: deleted %d GOPs for %s on %s", deleted, cameraID, d.Format("2006-01-02"))
		}
	}
}

func (r *RetentionCleaner) deleteExpiredFromIndex(ctx context.Context, cameraID string, start, end, cutoff time.Time) error {
	if deleter, ok := r.index.(interface {
		DeleteRange(ctx context.Context, cameraID string, start, end time.Time) error
	}); ok {
		return deleter.DeleteRange(ctx, cameraID, start, cutoff)
	}
	return nil
}
