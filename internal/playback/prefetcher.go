package playback

import (
	"context"
	"sync"

	"recording-server/internal/index"
	"recording-server/internal/recorder"
)

type Prefetcher struct {
	reader *FragmentReader
	mu     sync.Mutex
	cache  map[string][]recorder.Sample
}

func NewPrefetcher(reader *FragmentReader) *Prefetcher {
	return &Prefetcher{
		reader: reader,
		cache:  make(map[string][]recorder.Sample),
	}
}

func (p *Prefetcher) Prefetch(ctx context.Context, meta index.GOPMeta) {
	p.mu.Lock()
	if _, exists := p.cache[meta.ObjectKey]; exists {
		p.mu.Unlock()
		return
	}
	p.mu.Unlock()

	samples, err := p.reader.ReadGOP(ctx, meta)
	if err != nil {
		return
	}

	p.mu.Lock()
	p.cache[meta.ObjectKey] = samples
	if len(p.cache) > 10 {
		for k := range p.cache {
			delete(p.cache, k)
			break
		}
	}
	p.mu.Unlock()
}

func (p *Prefetcher) Get(key string) ([]recorder.Sample, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	samples, ok := p.cache[key]
	if ok {
		delete(p.cache, key)
	}
	return samples, ok
}

func (p *Prefetcher) Clear() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.cache = make(map[string][]recorder.Sample)
}
