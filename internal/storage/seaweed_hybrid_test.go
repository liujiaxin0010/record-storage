package storage

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"recording-server/internal/config"
)

type fakeMaster struct {
	volumeURL      string
	assignFailures int
	lookupURLs     []string
	lookupCalls    int
	pingFailures   int
}

func (f *fakeMaster) Assign(context.Context, AssignParams) (AssignResult, error) {
	if f.assignFailures > 0 {
		f.assignFailures--
		return AssignResult{}, errors.New("master unavailable")
	}
	return AssignResult{FID: "3,abc", VolumeID: "3", VolumeURL: f.volumeURL}, nil
}

func (f *fakeMaster) LookupVolume(context.Context, []string) (map[string][]VolumeLocation, error) {
	url := f.volumeURL
	if len(f.lookupURLs) > 0 {
		index := f.lookupCalls
		if index >= len(f.lookupURLs) {
			index = len(f.lookupURLs) - 1
		}
		url = f.lookupURLs[index]
		f.lookupCalls++
	}
	return map[string][]VolumeLocation{"3": {{URL: url}}}, nil
}

func (f *fakeMaster) Ping(context.Context) error {
	if f.pingFailures > 0 {
		f.pingFailures--
		return errors.New("master ping failed")
	}
	return nil
}

func (f *fakeMaster) Close() error { return nil }

type fakeFiler struct {
	entries         map[string]EntryMeta
	createFailures  int
	lookupFailures  int
	deleteFailures  int
	listFailures    int
	pingFailures    int
}

func (f *fakeFiler) CreateEntry(_ context.Context, meta EntryMeta) error {
	if f.createFailures > 0 {
		f.createFailures--
		return errors.New("filer unavailable")
	}
	f.entries[meta.Key] = meta
	return nil
}

func (f *fakeFiler) LookupEntry(_ context.Context, key string) (EntryMeta, error) {
	if f.lookupFailures > 0 {
		f.lookupFailures--
		return EntryMeta{}, errors.New("lookup failed")
	}
	meta, ok := f.entries[key]
	if !ok {
		return EntryMeta{}, ErrNotFound
	}
	return meta, nil
}

func (f *fakeFiler) DeleteEntry(_ context.Context, key string, _ bool) error {
	if f.deleteFailures > 0 {
		f.deleteFailures--
		return errors.New("delete failed")
	}
	delete(f.entries, key)
	return nil
}

func (f *fakeFiler) ListEntries(_ context.Context, prefix string, _ int, _ string) ([]EntryMeta, bool, error) {
	if f.listFailures > 0 {
		f.listFailures--
		return nil, false, errors.New("list failed")
	}
	var items []EntryMeta
	for key, meta := range f.entries {
		if strings.HasPrefix(key, prefix) {
			items = append(items, meta)
		}
	}
	return items, false, nil
}

func (f *fakeFiler) Ping(context.Context) error {
	if f.pingFailures > 0 {
		f.pingFailures--
		return errors.New("filer ping failed")
	}
	return nil
}

func (f *fakeFiler) Close() error { return nil }

type fakeVolume struct {
	objects     map[string][]byte
	putFailures int
	rangeMisses map[string]int
}

func (f *fakeVolume) Put(_ context.Context, volumeURL, fileID string, data []byte, _ string) error {
	if f.putFailures > 0 {
		f.putFailures--
		return errors.New("volume put failed")
	}
	f.objects[fileID] = append([]byte(nil), data...)
	return nil
}

func (f *fakeVolume) Get(_ context.Context, volumeURL, fileID string) ([]byte, error) {
	if f.rangeMisses[volumeURL] > 0 {
		f.rangeMisses[volumeURL]--
		return nil, ErrNotFound
	}
	data, ok := f.objects[fileID]
	if !ok {
		return nil, ErrNotFound
	}
	return append([]byte(nil), data...), nil
}

func (f *fakeVolume) GetRange(_ context.Context, volumeURL, fileID string, offset, length int64) ([]byte, error) {
	data, err := f.Get(context.Background(), volumeURL, fileID)
	if err != nil {
		return nil, err
	}
	end := offset + length
	if end > int64(len(data)) {
		end = int64(len(data))
	}
	return append([]byte(nil), data[offset:end]...), nil
}

func TestSeaweedHybridStoragePutAndGetRange(t *testing.T) {
	objects := map[string][]byte{}
	volumeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := strings.TrimPrefix(r.URL.Path, "/")
		switch r.Method {
		case http.MethodPut:
			data := make([]byte, r.ContentLength)
			_, _ = r.Body.Read(data)
			objects[key] = data
			w.WriteHeader(http.StatusCreated)
		case http.MethodGet:
			if value, ok := objects[key]; ok {
				_, _ = w.Write(value)
				return
			}
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer volumeServer.Close()
	fallbackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
	}))
	defer fallbackServer.Close()

	store := newSeaweedHybridStorageWithClients(
		&fakeMaster{volumeURL: volumeServer.URL},
		&fakeFiler{entries: map[string]EntryMeta{}},
		NewHTTPVolumeClient(),
		NewFilerHTTPStorage(fallbackServer.URL),
		config.SeaweedFSConfig{Cache: config.SeaweedCacheConfig{EntryTTLSeconds: 60, VolumeTTLSeconds: 60}},
	)
	defer store.Close()

	if err := store.Put(context.Background(), "recordings/cam01/0001.frag", []byte("abcdef")); err != nil {
		t.Fatalf("Put() error = %v", err)
	}
	data, err := store.GetRange(context.Background(), "recordings/cam01/0001.frag", 0, 6)
	if err != nil {
		t.Fatalf("GetRange() error = %v", err)
	}
	if string(data) != "abcdef" {
		t.Fatalf("unexpected data: %s", string(data))
	}
	keys, err := store.List(context.Background(), "recordings/cam01")
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	if len(keys) != 1 || keys[0] != "recordings/cam01/0001.frag" {
		t.Fatalf("unexpected keys: %v", keys)
	}
	if err := store.Delete(context.Background(), "recordings/cam01/0001.frag"); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	if _, ok := objects["3,abc"]; !ok {
		t.Fatalf("volume object was not written: %v", fmt.Sprintf("%v", objects))
	}
}

func TestSeaweedHybridStorageRetriesTransientFailures(t *testing.T) {
	fallbackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusCreated) }))
	defer fallbackServer.Close()
	volume := &fakeVolume{objects: map[string][]byte{}, putFailures: 2, rangeMisses: map[string]int{}}
	master := &fakeMaster{volumeURL: "http://volume" , assignFailures: 2}
	filer := &fakeFiler{entries: map[string]EntryMeta{}, createFailures: 2}
	store := newSeaweedHybridStorageWithClients(master, filer, volume, NewFilerHTTPStorage(fallbackServer.URL), config.SeaweedFSConfig{Cache: config.SeaweedCacheConfig{EntryTTLSeconds: 60, VolumeTTLSeconds: 60}})
	defer store.Close()

	if err := store.Put(context.Background(), "recordings/cam01/0002.frag", []byte("payload")); err != nil {
		t.Fatalf("expected Put() to succeed after retries, got %v", err)
	}
	if _, ok := volume.objects["3,abc"]; !ok {
		t.Fatal("expected object to be persisted after retries")
	}
	if _, ok := filer.entries["recordings/cam01/0002.frag"]; !ok {
		t.Fatal("expected entry to be created after retries")
	}
}

func TestSeaweedHybridStorageRefreshesVolumeRouteOnNotFound(t *testing.T) {
	fallbackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusCreated) }))
	defer fallbackServer.Close()
	volume := &fakeVolume{objects: map[string][]byte{"3,abc": []byte("abcdef")}, rangeMisses: map[string]int{"http://bad": 1}}
	master := &fakeMaster{volumeURL: "http://good", lookupURLs: []string{"http://bad", "http://good"}}
	filer := &fakeFiler{entries: map[string]EntryMeta{"recordings/cam01/0003.frag": {Key: "recordings/cam01/0003.frag", FileID: "3,abc", VolumeID: "3"}}}
	store := newSeaweedHybridStorageWithClients(master, filer, volume, NewFilerHTTPStorage(fallbackServer.URL), config.SeaweedFSConfig{Cache: config.SeaweedCacheConfig{EntryTTLSeconds: 60, VolumeTTLSeconds: 60}})
	defer store.Close()

	data, err := store.GetRange(context.Background(), "recordings/cam01/0003.frag", 0, 6)
	if err != nil {
		t.Fatalf("GetRange() error = %v", err)
	}
	if string(data) != "abcdef" {
		t.Fatalf("unexpected data after route refresh: %s", string(data))
	}
	if master.lookupCalls < 2 {
		t.Fatalf("expected route lookup refresh, got %d lookup call(s)", master.lookupCalls)
	}
}

func TestSeaweedHybridStorageProbeControlsRecovery(t *testing.T) {
	fallbackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusCreated) }))
	defer fallbackServer.Close()
	master := &fakeMaster{volumeURL: "http://good"}
	filer := &fakeFiler{entries: map[string]EntryMeta{}, pingFailures: 1}
	store := newSeaweedHybridStorageWithClients(master, filer, &fakeVolume{objects: map[string][]byte{}, rangeMisses: map[string]int{}}, NewFilerHTTPStorage(fallbackServer.URL), config.SeaweedFSConfig{Cache: config.SeaweedCacheConfig{EntryTTLSeconds: 60, VolumeTTLSeconds: 60}})
	defer store.Close()

	store.mu.Lock()
	store.degraded = true
	store.nextProbe = time.Now().Add(-time.Second)
	store.mu.Unlock()

	if !store.shouldUseFallback() {
		t.Fatal("expected store to remain degraded when probe fails")
	}
	store.mu.Lock()
	store.nextProbe = time.Now().Add(-time.Second)
	store.mu.Unlock()
	if store.shouldUseFallback() {
		t.Fatal("expected store to recover after successful probe")
	}
}
