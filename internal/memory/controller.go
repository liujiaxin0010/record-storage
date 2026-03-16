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

func (c *Controller) Used() int64 {
	return atomic.LoadInt64(&c.usedBytes)
}

func (c *Controller) Max() int64 {
	return c.maxBytes
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

// ShouldDropFrame returns true if the given frame type should be dropped
// under the current memory pressure level.
// Level 0: no drops. Level 1: no drops (accelerate flush handled elsewhere).
// Level 2: drop B-frames. Level 3: drop B and P frames (I-frames only).
func (c *Controller) ShouldDropFrame(frameType string) bool {
	level := c.GetPressureLevel()
	switch level {
	case 2:
		return frameType == "B"
	case 3:
		return frameType == "B" || frameType == "P"
	default:
		return false
	}
}

var ErrMemoryLimit = &memoryError{"memory limit exceeded"}

type memoryError struct {
	msg string
}

func (e *memoryError) Error() string {
	return e.msg
}
