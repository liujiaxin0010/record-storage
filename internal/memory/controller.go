package memory

import (
	"runtime"
	"sync/atomic"
)

type Controller struct {
	maxBytes  int64
	usedBytes int64
}

func NewController(maxMB int) *Controller {
	return &Controller{
		maxBytes: int64(maxMB) * 1024 * 1024,
	}
}

func (c *Controller) Allocate(size int64) error {
	newUsed := atomic.AddInt64(&c.usedBytes, size)
	if newUsed > c.maxBytes {
		atomic.AddInt64(&c.usedBytes, -size)
		return ErrMemoryLimit
	}
	return nil
}

func (c *Controller) Release(size int64) {
	atomic.AddInt64(&c.usedBytes, -size)
}

func (c *Controller) GetPressureLevel() int {
	used := atomic.LoadInt64(&c.usedBytes)
	ratio := float64(used) / float64(c.maxBytes)
	if ratio < 0.6 {
		return 0
	} else if ratio < 0.8 {
		return 1
	} else if ratio < 0.9 {
		return 2
	}
	return 3
}

func (c *Controller) UpdateMetrics() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	atomic.StoreInt64(&c.usedBytes, int64(m.Alloc))
}

var ErrMemoryLimit = &memoryError{"memory limit exceeded"}

type memoryError struct {
	msg string
}

func (e *memoryError) Error() string {
	return e.msg
}
