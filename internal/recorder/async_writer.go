package recorder

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"recording-server/internal/index"
	"recording-server/internal/memory"
	"recording-server/internal/metrics"
	"recording-server/internal/storage"
)

type BackpressureStrategy string

const (
	BackpressureBlock      BackpressureStrategy = "block"
	BackpressureDrop       BackpressureStrategy = "drop"
	BackpressureDropOldest BackpressureStrategy = "drop_oldest"
)

type AsyncWriter struct {
	storage  storage.StorageBackend
	index    index.IndexStore
	queue    chan *WriteJob
	strategy BackpressureStrategy
	memCtrl  *memory.Controller

	mu     sync.RWMutex
	closed bool
	close  sync.Once
	wg     sync.WaitGroup

	submitted uint64
	failed    uint64
	dropped   uint64
}

type WriteJob struct {
	CameraID string
	GOPData  []byte
	InitData []byte
	Meta     index.GOPMeta
}

func NewAsyncWriter(store storage.StorageBackend, idx index.IndexStore, workers, queueSize int) *AsyncWriter {
	return NewAsyncWriterWithStrategy(store, idx, workers, queueSize, BackpressureBlock)
}

func NewAsyncWriterWithStrategy(store storage.StorageBackend, idx index.IndexStore, workers, queueSize int, strategy BackpressureStrategy) *AsyncWriter {
	w := &AsyncWriter{
		storage:  store,
		index:    idx,
		queue:    make(chan *WriteJob, queueSize),
		strategy: strategy,
	}
	for i := 0; i < workers; i++ {
		w.wg.Add(1)
		go w.worker()
	}
	return w
}

// SetMemoryController attaches a memory controller for tracking write buffer allocations.
func (w *AsyncWriter) SetMemoryController(mc *memory.Controller) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.memCtrl = mc
}

func (w *AsyncWriter) Submit(job *WriteJob) error {
	w.mu.RLock()
	closed := w.closed
	strategy := w.strategy
	memCtrl := w.memCtrl
	w.mu.RUnlock()
	if closed {
		return fmt.Errorf("writer closed")
	}

	// Track memory allocation for the job data
	jobSize := int64(len(job.GOPData) + len(job.InitData))
	if memCtrl != nil {
		if err := memCtrl.Allocate(jobSize); err != nil {
			atomic.AddUint64(&w.dropped, 1)
			metrics.WriterDropped.Inc()
			log.Printf("[%s] memory limit, dropping job (%d bytes)", job.CameraID, jobSize)
			return err
		}
	}

	atomic.AddUint64(&w.submitted, 1)
	metrics.WriterQueueDepth.Set(float64(len(w.queue) + 1))

	switch strategy {
	case BackpressureBlock:
		w.queue <- job
		return nil
	case BackpressureDrop:
		select {
		case w.queue <- job:
			return nil
		default:
			atomic.AddUint64(&w.dropped, 1)
			log.Printf("[%s] queue full, dropping job", job.CameraID)
			return fmt.Errorf("queue full")
		}
	case BackpressureDropOldest:
		select {
		case w.queue <- job:
			return nil
		default:
			select {
			case <-w.queue:
				atomic.AddUint64(&w.dropped, 1)
				log.Printf("[%s] queue full, dropped oldest job", job.CameraID)
			default:
			}
			w.queue <- job
			return nil
		}
	default:
		select {
		case w.queue <- job:
			return nil
		default:
			atomic.AddUint64(&w.dropped, 1)
			return fmt.Errorf("queue full")
		}
	}
}

func (w *AsyncWriter) WriteEvent(cameraID, eventType string, metadata map[string]string) error {
	return w.index.WriteEvent(context.Background(), cameraID, index.RecordingEvent{
		Type:      eventType,
		Timestamp: time.Now(),
		Metadata:  metadata,
	})
}

func (w *AsyncWriter) worker() {
	defer w.wg.Done()
	const maxBatchSize = 10
	batch := make([]*WriteJob, 0, maxBatchSize)

	for job := range w.queue {
		if job == nil {
			continue
		}
		batch = append(batch, job)

		// Try to collect more jobs without blocking
		for len(batch) < maxBatchSize {
			select {
			case nextJob, ok := <-w.queue:
				if !ok {
					goto processBatch
				}
				if nextJob != nil {
					batch = append(batch, nextJob)
				}
			default:
				goto processBatch
			}
		}

	processBatch:
		if err := w.processBatch(batch); err != nil {
			atomic.AddUint64(&w.failed, uint64(len(batch)))
			for _, j := range batch {
				log.Printf("[%s] failed to process job: %v", j.CameraID, err)
			}
		}
		// Release memory and update metrics for processed jobs
		for _, j := range batch {
			if w.memCtrl != nil {
				w.memCtrl.Release(int64(len(j.GOPData) + len(j.InitData)))
			}
		}
		metrics.WriterQueueDepth.Set(float64(len(w.queue)))
		batch = batch[:0]
	}
}

func (w *AsyncWriter) processJob(job *WriteJob) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if len(job.InitData) > 0 && job.Meta.InitKey != "" {
		if err := w.storage.Put(ctx, job.Meta.InitKey, job.InitData); err != nil && err != storage.ErrEntryPendingCompensation {
			return fmt.Errorf("put init: %w", err)
		}
	}
	if err := w.storage.Put(ctx, job.Meta.ObjectKey, job.GOPData); err != nil && err != storage.ErrEntryPendingCompensation {
		return fmt.Errorf("put gop: %w", err)
	}
	if err := w.index.WriteGOP(ctx, job.CameraID, job.Meta); err != nil {
		return fmt.Errorf("write index: %w", err)
	}
	return nil
}

func (w *AsyncWriter) processBatch(jobs []*WriteJob) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for _, job := range jobs {
		start := time.Now()
		if len(job.InitData) > 0 && job.Meta.InitKey != "" {
			if err := w.storage.Put(ctx, job.Meta.InitKey, job.InitData); err != nil && err != storage.ErrEntryPendingCompensation {
				return fmt.Errorf("put init: %w", err)
			}
		}
		if err := w.storage.Put(ctx, job.Meta.ObjectKey, job.GOPData); err != nil && err != storage.ErrEntryPendingCompensation {
			return fmt.Errorf("put gop: %w", err)
		}
		metrics.StorageWriteLatency.Observe(time.Since(start).Seconds())
		metrics.GOPsWritten.WithLabelValues(job.CameraID).Inc()
	}

	items := make([]struct {
		CameraID string
		Meta     index.GOPMeta
	}, len(jobs))
	for i, job := range jobs {
		items[i].CameraID = job.CameraID
		items[i].Meta = job.Meta
	}

	if batchIndex, ok := w.index.(interface {
		BatchWriteGOP(context.Context, []struct {
			CameraID string
			Meta     index.GOPMeta
		}) error
	}); ok {
		return batchIndex.BatchWriteGOP(ctx, items)
	}

	for _, job := range jobs {
		if err := w.index.WriteGOP(ctx, job.CameraID, job.Meta); err != nil {
			return fmt.Errorf("write index: %w", err)
		}
	}
	return nil
}

func (w *AsyncWriter) Close() error {
	w.close.Do(func() {
		w.mu.Lock()
		w.closed = true
		close(w.queue)
		w.mu.Unlock()
		w.wg.Wait()
	})
	return nil
}

func (w *AsyncWriter) Stats() (submitted, failed, dropped uint64, queueLen int) {
	return atomic.LoadUint64(&w.submitted),
		atomic.LoadUint64(&w.failed),
		atomic.LoadUint64(&w.dropped),
		len(w.queue)
}
