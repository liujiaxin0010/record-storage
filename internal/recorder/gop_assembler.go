package recorder

import (
	"fmt"
	"sync"
	"time"
)

type GOPAssembler struct {
	mu          sync.Mutex
	samples     []Sample
	hasKeyFrame bool
	startTime   time.Time
}

func NewGOPAssembler() *GOPAssembler {
	return &GOPAssembler{samples: make([]Sample, 0, 128)}
}

func (g *GOPAssembler) AddSample(sample Sample) (bool, error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	if sample.IsKey && g.hasKeyFrame && len(g.samples) > 0 {
		return true, nil
	}
	if !g.hasKeyFrame && !sample.IsKey {
		return false, nil
	}
	if !g.hasKeyFrame {
		g.hasKeyFrame = true
		g.startTime = sample.Timestamp
	}
	g.samples = append(g.samples, copySample(sample))
	return false, nil
}

func (g *GOPAssembler) Flush() ([]Sample, time.Time, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.hasKeyFrame || len(g.samples) == 0 {
		return nil, time.Time{}, fmt.Errorf("no gop available")
	}
	items := make([]Sample, len(g.samples))
	copy(items, g.samples)
	startTime := g.startTime
	g.samples = g.samples[:0]
	g.hasKeyFrame = false
	g.startTime = time.Time{}
	return items, startTime, nil
}

func (g *GOPAssembler) Reset() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.samples = g.samples[:0]
	g.hasKeyFrame = false
	g.startTime = time.Time{}
}

func copySample(sample Sample) Sample {
	clone := sample
	clone.Units = make([][]byte, len(sample.Units))
	for i, unit := range sample.Units {
		clone.Units[i] = append([]byte(nil), unit...)
	}
	if len(sample.Params.SPS) > 0 {
		clone.Params.SPS = append([]byte(nil), sample.Params.SPS...)
	}
	if len(sample.Params.PPS) > 0 {
		clone.Params.PPS = append([]byte(nil), sample.Params.PPS...)
	}
	if len(sample.Params.VPS) > 0 {
		clone.Params.VPS = append([]byte(nil), sample.Params.VPS...)
	}
	if len(sample.Params.Config) > 0 {
		clone.Params.Config = append([]byte(nil), sample.Params.Config...)
	}
	return clone
}

