package storage

import (
	"context"
	"crypto/md5"
	"errors"
	"fmt"
	"path"
	"strings"
	"sync"
	"time"

	"recording-server/internal/config"
)

const (
	seaweedRetryAttempts = 3
	seaweedRetryBase     = 50 * time.Millisecond
	seaweedRetryMax      = 1 * time.Second
)

type SeaweedHybridStorage struct {
	master   MasterClient
	filer    FilerClient
	volume   VolumeClient
	fallback *FilerHTTPStorage

	entryTTL  time.Duration
	volumeTTL time.Duration

	mu           sync.RWMutex
	entryCache   map[string]cacheEntry[EntryMeta]
	volumeCache  map[string]cacheEntry[[]VolumeLocation]
	compMu       sync.Mutex
	compPending  map[string]EntryMeta
	compQueue    chan EntryMeta
	ctx          context.Context
	cancel       context.CancelFunc
	wg           sync.WaitGroup
	failureCount int
	windowStart  time.Time
	degraded     bool
	nextProbe    time.Time
}

func NewSeaweedHybridStorage(cfg config.SeaweedFSConfig) (*SeaweedHybridStorage, error) {
	master, err := NewGRPCMasterClient(cfg.MasterGRPCAddrs)
	if err != nil {
		return nil, err
	}
	filer, err := NewGRPCFilerClient(cfg.FilerGRPCAddrs)
	if err != nil {
		master.Close()
		return nil, err
	}
	return newSeaweedHybridStorageWithClients(master, filer, NewHTTPVolumeClient(), NewFilerHTTPStorage(cfg.FilerHTTPURL), cfg), nil
}

func newSeaweedHybridStorageWithClients(master MasterClient, filer FilerClient, volume VolumeClient, fallback *FilerHTTPStorage, cfg config.SeaweedFSConfig) *SeaweedHybridStorage {
	ctx, cancel := context.WithCancel(context.Background())
	s := &SeaweedHybridStorage{
		master:      master,
		filer:       filer,
		volume:      volume,
		fallback:    fallback,
		entryTTL:    time.Duration(cfg.Cache.EntryTTLSeconds) * time.Second,
		volumeTTL:   time.Duration(cfg.Cache.VolumeTTLSeconds) * time.Second,
		entryCache:  make(map[string]cacheEntry[EntryMeta]),
		volumeCache: make(map[string]cacheEntry[[]VolumeLocation]),
		compPending: make(map[string]EntryMeta),
		compQueue:   make(chan EntryMeta, 1024),
		ctx:         ctx,
		cancel:      cancel,
	}
	s.wg.Add(1)
	go s.runCompensationLoop()
	return s
}

func (s *SeaweedHybridStorage) Put(ctx context.Context, key string, data []byte) error {
	if s.shouldUseFallback() {
		return s.fallback.Put(ctx, key, data)
	}
	assign, err := retryValue(ctx, seaweedRetryAttempts, func() (AssignResult, error) {
		return s.master.Assign(ctx, AssignParams{Count: 1})
	})
	if err != nil {
		s.recordControlFailure()
		return s.tryFallbackPut(ctx, key, data, err)
	}
	if err := retryDo(ctx, seaweedRetryAttempts, func() error {
		return s.volume.Put(withTimeout(ctx, 2*time.Second), assign.VolumeURL, assign.FID, data, assign.AuthToken)
	}); err != nil {
		return err
	}
	now := time.Now().UnixNano()
	meta := EntryMeta{
		Key:        key,
		FileID:     assign.FID,
		VolumeID:   assign.VolumeID,
		Size:       int64(len(data)),
		MimeType:   detectMimeType(key),
		CreatedAt:  now,
		ModifiedAt: now,
		ETag:       fmt.Sprintf("%x", md5.Sum(data)),
	}
	if err := retryDo(ctx, seaweedRetryAttempts, func() error {
		return s.filer.CreateEntry(withTimeout(ctx, 300*time.Millisecond), meta)
	}); err != nil {
		s.recordControlFailure()
		s.enqueueCompensation(meta)
		return ErrEntryPendingCompensation
	}
	s.clearControlFailures()
	s.setEntryCache(meta)
	s.setVolumeCache(assign.VolumeID, []VolumeLocation{{URL: assign.VolumeURL, AuthToken: assign.AuthToken}})
	return nil
}

func (s *SeaweedHybridStorage) Get(ctx context.Context, key string) ([]byte, error) {
	if s.shouldUseFallback() {
		return s.fallback.Get(ctx, key)
	}
	meta, locations, err := s.resolveEntryAndVolume(ctx, key)
	if err != nil {
		return nil, err
	}
	data, err := retryValue(ctx, seaweedRetryAttempts, func() ([]byte, error) {
		return s.volume.Get(withTimeout(ctx, 2*time.Second), locations[0].URL, meta.FileID)
	})
	if errors.Is(err, ErrNotFound) {
		s.deleteVolumeCache(meta.VolumeID)
		if locations, err = s.getVolumeLocationsFresh(ctx, meta.VolumeID); err == nil && len(locations) > 0 {
			return s.volume.Get(withTimeout(ctx, 2*time.Second), locations[0].URL, meta.FileID)
		}
	}
	return data, err
}

func (s *SeaweedHybridStorage) GetRange(ctx context.Context, key string, offset, length int64) ([]byte, error) {
	if s.shouldUseFallback() {
		return s.fallback.GetRange(ctx, key, offset, length)
	}
	meta, locations, err := s.resolveEntryAndVolume(ctx, key)
	if err != nil {
		return nil, err
	}
	data, err := retryValue(ctx, seaweedRetryAttempts, func() ([]byte, error) {
		return s.volume.GetRange(withTimeout(ctx, 2*time.Second), locations[0].URL, meta.FileID, offset, length)
	})
	if errors.Is(err, ErrNotFound) {
		s.deleteVolumeCache(meta.VolumeID)
		if locations, err = s.getVolumeLocationsFresh(ctx, meta.VolumeID); err == nil && len(locations) > 0 {
			return s.volume.GetRange(withTimeout(ctx, 2*time.Second), locations[0].URL, meta.FileID, offset, length)
		}
	}
	return data, err
}

func (s *SeaweedHybridStorage) Delete(ctx context.Context, key string) error {
	if s.shouldUseFallback() {
		return s.fallback.Delete(ctx, key)
	}
	if err := retryDo(ctx, seaweedRetryAttempts, func() error {
		return s.filer.DeleteEntry(withTimeout(ctx, 300*time.Millisecond), key, false)
	}); err != nil {
		s.recordControlFailure()
		return s.fallback.Delete(ctx, key)
	}
	s.mu.Lock()
	delete(s.entryCache, key)
	s.mu.Unlock()
	s.clearControlFailures()
	return nil
}

func (s *SeaweedHybridStorage) List(ctx context.Context, prefix string) ([]string, error) {
	if s.shouldUseFallback() {
		return s.fallback.List(ctx, prefix)
	}
	entries, _, err := retryValue3(ctx, seaweedRetryAttempts, func() ([]EntryMeta, bool, error) {
		return s.filer.ListEntries(withTimeout(ctx, 300*time.Millisecond), prefix, 1000, "")
	})
	if err != nil {
		s.recordControlFailure()
		return s.fallback.List(ctx, prefix)
	}
	keys := make([]string, 0, len(entries))
	for _, entry := range entries {
		keys = append(keys, entry.Key)
	}
	return keys, nil
}

func (s *SeaweedHybridStorage) Close() error {
	s.cancel()
	close(s.compQueue)
	s.wg.Wait()
	var errs []string
	if s.master != nil {
		if err := s.master.Close(); err != nil {
			errs = append(errs, err.Error())
		}
	}
	if s.filer != nil {
		if err := s.filer.Close(); err != nil {
			errs = append(errs, err.Error())
		}
	}
	if s.fallback != nil {
		if err := s.fallback.Close(); err != nil {
			errs = append(errs, err.Error())
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf(strings.Join(errs, "; "))
	}
	return nil
}

func (s *SeaweedHybridStorage) resolveEntryAndVolume(ctx context.Context, key string) (EntryMeta, []VolumeLocation, error) {
	meta, err := s.getEntry(key)
	if err != nil {
		return EntryMeta{}, nil, err
	}
	volumeID := meta.VolumeID
	if volumeID == "" {
		volumeID = parseVolumeID(meta.FileID)
	}
	locations, err := s.getVolumeLocations(ctx, volumeID)
	if err != nil {
		return EntryMeta{}, nil, err
	}
	if len(locations) == 0 {
		return EntryMeta{}, nil, ErrVolumeRouteNotFound
	}
	return meta, locations, nil
}

func (s *SeaweedHybridStorage) getEntry(key string) (EntryMeta, error) {
	if meta, ok := s.cachedEntry(key); ok {
		return meta, nil
	}
	meta, err := retryValue(context.Background(), seaweedRetryAttempts, func() (EntryMeta, error) {
		return s.filer.LookupEntry(withTimeout(context.Background(), 300*time.Millisecond), key)
	})
	if err != nil {
		return EntryMeta{}, err
	}
	s.setEntryCache(meta)
	return meta, nil
}

func (s *SeaweedHybridStorage) getVolumeLocations(ctx context.Context, volumeID string) ([]VolumeLocation, error) {
	if locations, ok := s.cachedVolume(volumeID); ok {
		return locations, nil
	}
	return s.getVolumeLocationsFresh(ctx, volumeID)
	}

func (s *SeaweedHybridStorage) getVolumeLocationsFresh(ctx context.Context, volumeID string) ([]VolumeLocation, error) {
	result, err := retryValue(ctx, seaweedRetryAttempts, func() (map[string][]VolumeLocation, error) {
		return s.master.LookupVolume(withTimeout(ctx, 200*time.Millisecond), []string{volumeID})
	})
	if err != nil {
		return nil, err
	}
	locations := result[volumeID]
	s.setVolumeCache(volumeID, locations)
	return locations, nil
}

func (s *SeaweedHybridStorage) shouldUseFallback() bool {
	s.mu.RLock()
	degraded := s.degraded
	nextProbe := s.nextProbe
	s.mu.RUnlock()
	if !degraded {
		return false
	}
	if time.Now().Before(nextProbe) {
		return true
	}
	if err := s.probeControlPlane(); err != nil {
		s.mu.Lock()
		s.degraded = true
		s.nextProbe = time.Now().Add(5 * time.Second)
		s.mu.Unlock()
		return true
	}
	s.clearControlFailures()
	return false
}

func (s *SeaweedHybridStorage) recordControlFailure() {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	if s.windowStart.IsZero() || now.Sub(s.windowStart) > 30*time.Second {
		s.windowStart = now
		s.failureCount = 1
	} else {
		s.failureCount++
	}
	if s.failureCount >= 20 {
		s.degraded = true
		s.nextProbe = now.Add(5 * time.Second)
	}
}

func (s *SeaweedHybridStorage) clearControlFailures() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failureCount = 0
	s.windowStart = time.Time{}
	s.degraded = false
	s.nextProbe = time.Time{}
}

func (s *SeaweedHybridStorage) probeControlPlane() error {
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if err := s.master.Ping(ctx); err != nil {
		return err
	}
	if err := s.filer.Ping(ctx); err != nil {
		return err
	}
	return nil
}

func (s *SeaweedHybridStorage) tryFallbackPut(ctx context.Context, key string, data []byte, err error) error {
	if fbErr := s.fallback.Put(ctx, key, data); fbErr != nil {
		return fmt.Errorf("primary put failed: %v; fallback put failed: %w", err, fbErr)
	}
	return ErrStorageDegraded
}

func (s *SeaweedHybridStorage) enqueueCompensation(meta EntryMeta) {
	s.compMu.Lock()
	defer s.compMu.Unlock()
	if _, ok := s.compPending[meta.Key]; ok {
		return
	}
	s.compPending[meta.Key] = meta
	select {
	case s.compQueue <- meta:
	default:
	}
}

func (s *SeaweedHybridStorage) runCompensationLoop() {
	defer s.wg.Done()
	for {
		select {
		case <-s.ctx.Done():
			return
		case meta, ok := <-s.compQueue:
			if !ok {
				return
			}
			s.retryCreateEntry(meta)
		}
	}
}

func (s *SeaweedHybridStorage) retryCreateEntry(meta EntryMeta) {
	for attempt := 0; attempt < 3; attempt++ {
		if err := s.filer.CreateEntry(withTimeout(context.Background(), 300*time.Millisecond), meta); err == nil {
			s.compMu.Lock()
			delete(s.compPending, meta.Key)
			s.compMu.Unlock()
			s.setEntryCache(meta)
			return
		}
		time.Sleep(time.Duration(50*(1<<attempt)) * time.Millisecond)
	}
}

func (s *SeaweedHybridStorage) cachedEntry(key string) (EntryMeta, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	entry, ok := s.entryCache[key]
	if !ok || time.Now().After(entry.ExpiresAt) {
		return EntryMeta{}, false
	}
	return entry.Value, true
}

func (s *SeaweedHybridStorage) cachedVolume(volumeID string) ([]VolumeLocation, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	entry, ok := s.volumeCache[volumeID]
	if !ok || time.Now().After(entry.ExpiresAt) {
		return nil, false
	}
	return entry.Value, true
}

func (s *SeaweedHybridStorage) setEntryCache(meta EntryMeta) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entryCache[meta.Key] = cacheEntry[EntryMeta]{Value: meta, ExpiresAt: time.Now().Add(s.entryTTL)}
}

func (s *SeaweedHybridStorage) setVolumeCache(volumeID string, locations []VolumeLocation) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.volumeCache[volumeID] = cacheEntry[[]VolumeLocation]{Value: locations, ExpiresAt: time.Now().Add(s.volumeTTL)}
}

func (s *SeaweedHybridStorage) deleteVolumeCache(volumeID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.volumeCache, volumeID)
}

func detectMimeType(key string) string {
	if strings.HasSuffix(key, ".mp4init") || strings.HasSuffix(key, ".frag") {
		return "video/mp4"
	}
	return "application/octet-stream"
}

func withTimeout(ctx context.Context, timeout time.Duration) context.Context {
	if _, ok := ctx.Deadline(); ok {
		return ctx
	}
	nctx, _ := context.WithTimeout(ctx, timeout)
	return nctx
}

func retryDo(ctx context.Context, attempts int, fn func() error) error {
	_, err := retryValue(ctx, attempts, func() (struct{}, error) {
		return struct{}{}, fn()
	})
	return err
}

func retryValue[T any](ctx context.Context, attempts int, fn func() (T, error)) (T, error) {
	var zero T
	var lastErr error
	for attempt := 0; attempt < attempts; attempt++ {
		value, err := fn()
		if err == nil {
			return value, nil
		}
		lastErr = err
		if !isRetryableError(err) || attempt == attempts-1 {
			return zero, err
		}
		wait := seaweedRetryBase * time.Duration(1<<attempt)
		if wait > seaweedRetryMax {
			wait = seaweedRetryMax
		}
		select {
		case <-ctx.Done():
			return zero, ctx.Err()
		case <-time.After(wait):
		}
	}
	return zero, lastErr
}

func retryValue3[A any](ctx context.Context, attempts int, fn func() (A, bool, error)) (A, bool, error) {
	var zero A
	var lastErr error
	for attempt := 0; attempt < attempts; attempt++ {
		value, more, err := fn()
		if err == nil {
			return value, more, nil
		}
		lastErr = err
		if !isRetryableError(err) || attempt == attempts-1 {
			return zero, false, err
		}
		wait := seaweedRetryBase * time.Duration(1<<attempt)
		if wait > seaweedRetryMax {
			wait = seaweedRetryMax
		}
		select {
		case <-ctx.Done():
			return zero, false, ctx.Err()
		case <-time.After(wait):
		}
	}
	return zero, false, lastErr
}

func isRetryableError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, ErrNotFound) || errors.Is(err, ErrInvalidFID) || errors.Is(err, context.Canceled) {
		return false
	}
	return true
}

func BuildObjectKey(cameraID string, ts time.Time, sequence uint64, suffix string) string {
	return path.Join("recordings", cameraID, ts.Format("2006-01-02"), fmt.Sprintf("%d-%06d%s", ts.UnixMilli(), sequence, suffix))
}
