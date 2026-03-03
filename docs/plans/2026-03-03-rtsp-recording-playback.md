# RTSP Recording & Playback System Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build a high-performance RTSP recording and playback system that records 500-1000 concurrent camera streams to SeaweedFS and provides RTSP-based playback with seek and speed control.

**Architecture:** Sequential phase implementation starting with basic recording (RTSP pull → GOP assembly → fMP4 packaging → SeaweedFS storage), then adding playback (RTSP server → fragment reading → RTP sending), followed by optimizations (hybrid storage, seek, speed control), robustness features (memory management, reconnection), and operational tooling (metrics, API).

**Tech Stack:** Go, gortsplib (RTSP), mp4ff (fMP4), BoltDB (indexing), SeaweedFS (storage), Prometheus (metrics)

---

## Phase 1: Basic Recording (Weeks 1-3)

### Task 1: Project Initialization

**Files:**
- Create: `go.mod`
- Create: `go.sum`
- Create: `cmd/server/main.go`
- Create: `.gitignore`

**Step 1: Initialize Go module**

```bash
go mod init recording-server
```

Expected: Creates `go.mod` with module name

**Step 2: Create main entry point**

Create `cmd/server/main.go`:

```go
package main

import (
	"fmt"
	"os"
)

func main() {
	fmt.Println("Recording Server starting...")
	os.Exit(0)
}
```

**Step 3: Create .gitignore**

Create `.gitignore`:

```
# Binaries
*.exe
*.exe~
*.dll
*.so
*.dylib
/recording-server
/server

# Test binary
*.test

# Output
*.out

# Go workspace
go.work

# Data
/data/
*.db

# Config
config.local.yaml

# IDE
.vscode/
.idea/
*.swp
*.swo
*~
```

**Step 4: Test build**

```bash
go build -o recording-server ./cmd/server
./recording-server
```

Expected: Prints "Recording Server starting..." and exits

**Step 5: Commit**

```bash
git add go.mod cmd/server/main.go .gitignore
git commit -m "feat: initialize Go project structure"
```

---

### Task 2: Configuration Module

**Files:**
- Create: `internal/config/config.go`
- Create: `internal/config/validator.go`
- Create: `internal/config/config_test.go`
- Create: `config.yaml`

**Step 1: Write test for config loading**

Create `internal/config/config_test.go`:

```go
package config

import (
	"os"
	"testing"
)

func TestLoadConfig(t *testing.T) {
	// Create temp config file
	content := `
server:
  rtsp_playback_port: 8554
  api_port: 8080
  metrics_port: 9090

memory:
  max_memory_mb: 4096

storage:
  seaweedfs:
    mode: "filer_http"
    filer_http_url: "http://localhost:8888"

index:
  bolt_path: "/tmp/test.db"

recorder:
  async_writer_workers: 8
  async_writer_queue_size: 1000

cameras:
  - id: "cam01"
    rtsp_url: "rtsp://test:test@localhost:554/stream"
    transport: "tcp"
    enabled: true
`
	tmpfile, err := os.CreateTemp("", "config-*.yaml")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpfile.Name())

	if _, err := tmpfile.Write([]byte(content)); err != nil {
		t.Fatal(err)
	}
	tmpfile.Close()

	cfg, err := LoadConfig(tmpfile.Name())
	if err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}

	if cfg.Server.RTSPPlaybackPort != 8554 {
		t.Errorf("Expected port 8554, got %d", cfg.Server.RTSPPlaybackPort)
	}

	if len(cfg.Cameras) != 1 {
		t.Errorf("Expected 1 camera, got %d", len(cfg.Cameras))
	}

	if cfg.Cameras[0].ID != "cam01" {
		t.Errorf("Expected camera ID 'cam01', got '%s'", cfg.Cameras[0].ID)
	}
}
```

**Step 2: Run test to verify it fails**

```bash
go test ./internal/config -v
```

Expected: FAIL - "undefined: LoadConfig"

**Step 3: Add dependencies**

```bash
go get gopkg.in/yaml.v3
```

**Step 4: Implement config structures**

Create `internal/config/config.go`:

```go
package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Server   ServerConfig   `yaml:"server"`
	Memory   MemoryConfig   `yaml:"memory"`
	Storage  StorageConfig  `yaml:"storage"`
	Index    IndexConfig    `yaml:"index"`
	Recorder RecorderConfig `yaml:"recorder"`
	Cameras  []CameraConfig `yaml:"cameras"`
}

type ServerConfig struct {
	RTSPPlaybackPort int `yaml:"rtsp_playback_port"`
	APIPort          int `yaml:"api_port"`
	MetricsPort      int `yaml:"metrics_port"`
}

type MemoryConfig struct {
	MaxMemoryMB int `yaml:"max_memory_mb"`
}

type StorageConfig struct {
	SeaweedFS SeaweedFSConfig `yaml:"seaweedfs"`
}

type SeaweedFSConfig struct {
	Mode          string   `yaml:"mode"`
	FilerHTTPURL  string   `yaml:"filer_http_url"`
	MasterGRPCAddrs []string `yaml:"master_grpc_addrs"`
	FilerGRPCAddrs  []string `yaml:"filer_grpc_addrs"`
}

type IndexConfig struct {
	BoltPath string `yaml:"bolt_path"`
}

type RecorderConfig struct {
	AsyncWriterWorkers   int `yaml:"async_writer_workers"`
	AsyncWriterQueueSize int `yaml:"async_writer_queue_size"`
}

type CameraConfig struct {
	ID        string `yaml:"id"`
	RTSPURL   string `yaml:"rtsp_url"`
	Transport string `yaml:"transport"`
	Enabled   bool   `yaml:"enabled"`
}

func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config file: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}

	return &cfg, nil
}
```

**Step 5: Run test to verify it passes**

```bash
go test ./internal/config -v
```

Expected: PASS

**Step 6: Create example config file**

Create `config.yaml`:

```yaml
server:
  rtsp_playback_port: 8554
  api_port: 8080
  metrics_port: 9090

memory:
  max_memory_mb: 4096

storage:
  seaweedfs:
    mode: "filer_http"
    filer_http_url: "http://seaweedfs-filer:8888"

index:
  bolt_path: "/data/index/recordings.db"

recorder:
  async_writer_workers: 8
  async_writer_queue_size: 1000

cameras:
  - id: "cam01"
    rtsp_url: "rtsp://admin:password@192.168.1.64:554/Streaming/Channels/101"
    transport: "tcp"
    enabled: true
```

**Step 7: Commit**

```bash
git add internal/config/ config.yaml go.mod go.sum
git commit -m "feat: add configuration loading with YAML support"
```

---

### Task 3: Storage Interface & Filer HTTP Implementation

**Files:**
- Create: `internal/storage/interface.go`
- Create: `internal/storage/seaweed_filer_http.go`
- Create: `internal/storage/seaweed_filer_http_test.go`

**Step 1: Write storage interface**

Create `internal/storage/interface.go`:

```go
package storage

import (
	"context"
	"errors"
)

var (
	ErrNotFound = errors.New("object not found")
	ErrInvalidKey = errors.New("invalid key")
)

type StorageBackend interface {
	Put(ctx context.Context, key string, data []byte) error
	Get(ctx context.Context, key string) ([]byte, error)
	GetRange(ctx context.Context, key string, offset, length int64) ([]byte, error)
	Delete(ctx context.Context, key string) error
	List(ctx context.Context, prefix string) ([]string, error)
}
```

**Step 2: Write test for Filer HTTP storage**

Create `internal/storage/seaweed_filer_http_test.go`:

```go
package storage

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestSeaweedFilerHTTP_Put(t *testing.T) {
	var receivedData []byte
	var receivedPath string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost && r.Method != http.MethodPut {
			t.Errorf("Expected POST or PUT, got %s", r.Method)
		}
		receivedPath = r.URL.Path
		data, _ := io.ReadAll(r.Body)
		receivedData = data
		w.WriteHeader(http.StatusCreated)
	}))
	defer server.Close()

	storage := NewSeaweedFilerHTTP(server.URL)

	testData := []byte("test data")
	err := storage.Put(context.Background(), "test/key.dat", testData)

	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	if receivedPath != "/test/key.dat" {
		t.Errorf("Expected path '/test/key.dat', got '%s'", receivedPath)
	}

	if string(receivedData) != string(testData) {
		t.Errorf("Expected data '%s', got '%s'", testData, receivedData)
	}
}

func TestSeaweedFilerHTTP_Get(t *testing.T) {
	testData := []byte("response data")

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("Expected GET, got %s", r.Method)
		}
		w.WriteHeader(http.StatusOK)
		w.Write(testData)
	}))
	defer server.Close()

	storage := NewSeaweedFilerHTTP(server.URL)

	data, err := storage.Get(context.Background(), "test/key.dat")

	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if string(data) != string(testData) {
		t.Errorf("Expected data '%s', got '%s'", testData, data)
	}
}
```

**Step 3: Run test to verify it fails**

```bash
go test ./internal/storage -v
```

Expected: FAIL - "undefined: NewSeaweedFilerHTTP"

**Step 4: Implement Filer HTTP storage**

Create `internal/storage/seaweed_filer_http.go`:

```go
package storage

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
)

type SeaweedFilerHTTP struct {
	baseURL string
	client  *http.Client
}

func NewSeaweedFilerHTTP(baseURL string) *SeaweedFilerHTTP {
	return &SeaweedFilerHTTP{
		baseURL: strings.TrimSuffix(baseURL, "/"),
		client:  &http.Client{},
	}
}

func (s *SeaweedFilerHTTP) Put(ctx context.Context, key string, data []byte) error {
	url := fmt.Sprintf("%s/%s", s.baseURL, strings.TrimPrefix(key, "/"))

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}

	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("http request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("unexpected status %d: %s", resp.StatusCode, body)
	}

	return nil
}

func (s *SeaweedFilerHTTP) Get(ctx context.Context, key string) ([]byte, error) {
	url := fmt.Sprintf("%s/%s", s.baseURL, strings.TrimPrefix(key, "/"))

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	resp, err := s.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("http request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, ErrNotFound
	}

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("unexpected status %d: %s", resp.StatusCode, body)
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}

	return data, nil
}

func (s *SeaweedFilerHTTP) GetRange(ctx context.Context, key string, offset, length int64) ([]byte, error) {
	url := fmt.Sprintf("%s/%s", s.baseURL, strings.TrimPrefix(key, "/"))

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", offset, offset+length-1))

	resp, err := s.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("http request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, ErrNotFound
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("unexpected status %d: %s", resp.StatusCode, body)
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}

	return data, nil
}

func (s *SeaweedFilerHTTP) Delete(ctx context.Context, key string) error {
	url := fmt.Sprintf("%s/%s", s.baseURL, strings.TrimPrefix(key, "/"))

	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, url, nil)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}

	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("http request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusNotFound {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("unexpected status %d: %s", resp.StatusCode, body)
	}

	return nil
}

func (s *SeaweedFilerHTTP) List(ctx context.Context, prefix string) ([]string, error) {
	// Simplified implementation - SeaweedFS Filer supports directory listing
	// This would need proper implementation based on Filer API
	return nil, fmt.Errorf("not implemented")
}
```

**Step 5: Run test to verify it passes**

```bash
go test ./internal/storage -v
```

Expected: PASS

**Step 6: Commit**

```bash
git add internal/storage/
git commit -m "feat: add storage interface and SeaweedFS Filer HTTP implementation"
```

---

### Task 4: Index Module with BoltDB

**Files:**
- Create: `internal/index/interface.go`
- Create: `internal/index/models.go`
- Create: `internal/index/bolt_index.go`
- Create: `internal/index/bolt_index_test.go`

**Step 1: Define index interface and models**

Create `internal/index/interface.go`:

```go
package index

import (
	"context"
	"time"
)

type IndexStore interface {
	WriteGOP(ctx context.Context, meta GOPMeta) error
	QueryRange(ctx context.Context, cameraID string, start, end time.Time) ([]GOPMeta, error)
	Close() error
}
```

Create `internal/index/models.go`:

```go
package index

import "time"

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
```

**Step 2: Add BoltDB dependency**

```bash
go get go.etcd.io/bbolt
```

**Step 3: Write test for BoltDB index**

Create `internal/index/bolt_index_test.go`:

```go
package index

import (
	"context"
	"os"
	"testing"
	"time"
)

func TestBoltIndex_WriteAndQuery(t *testing.T) {
	tmpfile, err := os.CreateTemp("", "index-*.db")
	if err != nil {
		t.Fatal(err)
	}
	tmpfile.Close()
	defer os.Remove(tmpfile.Name())

	idx, err := NewBoltIndex(tmpfile.Name())
	if err != nil {
		t.Fatalf("NewBoltIndex failed: %v", err)
	}
	defer idx.Close()

	now := time.Now()
	meta := GOPMeta{
		CameraID:   "cam01",
		StartTime:  now,
		EndTime:    now.Add(2 * time.Second),
		ObjectKey:  "recordings/cam01/data/2024-01-01/1234567890.frag",
		InitKey:    "recordings/cam01/init/h264.mp4init",
		Size:       1024000,
		FrameCount: 50,
		Codec:      "h264",
	}

	err = idx.WriteGOP(context.Background(), meta)
	if err != nil {
		t.Fatalf("WriteGOP failed: %v", err)
	}

	results, err := idx.QueryRange(context.Background(), "cam01", now.Add(-1*time.Hour), now.Add(1*time.Hour))
	if err != nil {
		t.Fatalf("QueryRange failed: %v", err)
	}

	if len(results) != 1 {
		t.Fatalf("Expected 1 result, got %d", len(results))
	}

	if results[0].ObjectKey != meta.ObjectKey {
		t.Errorf("Expected ObjectKey %s, got %s", meta.ObjectKey, results[0].ObjectKey)
	}
}
```

**Step 4: Run test (should fail)**

```bash
go test ./internal/index -v
```

Expected: FAIL - "undefined: NewBoltIndex"

**Step 5: Implement BoltDB index**

Create `internal/index/bolt_index.go`:

```go
package index

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	bolt "go.etcd.io/bbolt"
)

var gopBucket = []byte("gop_index")

type BoltIndex struct {
	db *bolt.DB
}

func NewBoltIndex(path string) (*BoltIndex, error) {
	db, err := bolt.Open(path, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return nil, fmt.Errorf("open bolt db: %w", err)
	}

	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(gopBucket)
		return err
	})
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("create bucket: %w", err)
	}

	return &BoltIndex{db: db}, nil
}

func (b *BoltIndex) WriteGOP(ctx context.Context, meta GOPMeta) error {
	key := fmt.Sprintf("%s:%d", meta.CameraID, meta.StartTime.UnixNano())

	data, err := json.Marshal(meta)
	if err != nil {
		return fmt.Errorf("marshal meta: %w", err)
	}

	return b.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(gopBucket)
		return bucket.Put([]byte(key), data)
	})
}

func (b *BoltIndex) QueryRange(ctx context.Context, cameraID string, start, end time.Time) ([]GOPMeta, error) {
	var results []GOPMeta

	minKey := fmt.Sprintf("%s:%d", cameraID, start.UnixNano())
	maxKey := fmt.Sprintf("%s:%d", cameraID, end.UnixNano())

	err := b.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(gopBucket)
		cursor := bucket.Cursor()

		for k, v := cursor.Seek([]byte(minKey)); k != nil && string(k) <= maxKey; k, v = cursor.Next() {
			var meta GOPMeta
			if err := json.Unmarshal(v, &meta); err != nil {
				return fmt.Errorf("unmarshal meta: %w", err)
			}
			results = append(results, meta)
		}

		return nil
	})

	return results, err
}

func (b *BoltIndex) Close() error {
	return b.db.Close()
}
```

**Step 6: Run test (should pass)**

```bash
go test ./internal/index -v
```

Expected: PASS

**Step 7: Commit**

```bash
git add internal/index/ go.mod go.sum
git commit -m "feat: add BoltDB index for GOP metadata storage"
```

---

### Task 5: GOP Assembler

**Files:**
- Create: `internal/recorder/gop_assembler.go`
- Create: `internal/recorder/gop_assembler_test.go`

**Step 1: Add media dependencies**

```bash
go get github.com/bluenviron/mediacommon/pkg/codecs/h264
go get github.com/bluenviron/mediacommon/pkg/codecs/h265
```

**Step 2: Write test for GOP assembler**

Create `internal/recorder/gop_assembler_test.go`:

```go
package recorder

import (
	"testing"
)

func TestGOPAssembler_DetectIFrame(t *testing.T) {
	assembler := NewGOPAssembler(2 * 1024 * 1024)

	// H.264 I-frame NALU (type 5)
	iframeNALU := []byte{0x00, 0x00, 0x00, 0x01, 0x65, 0x88, 0x84, 0x00}

	isKeyFrame := assembler.isH264KeyFrame(iframeNALU)
	if !isKeyFrame {
		t.Error("Expected I-frame to be detected as key frame")
	}

	// H.264 P-frame NALU (type 1)
	pframeNALU := []byte{0x00, 0x00, 0x00, 0x01, 0x41, 0x9a, 0x21, 0x00}

	isKeyFrame = assembler.isH264KeyFrame(pframeNALU)
	if isKeyFrame {
		t.Error("Expected P-frame to not be detected as key frame")
	}
}

func TestGOPAssembler_AddFrame(t *testing.T) {
	assembler := NewGOPAssembler(2 * 1024 * 1024)

	// Add I-frame
	iframe := []byte{0x00, 0x00, 0x00, 0x01, 0x65, 0x88, 0x84, 0x00}
	gop, complete := assembler.AddFrame(iframe, true)

	if complete {
		t.Error("First I-frame should not complete a GOP")
	}
	if gop != nil {
		t.Error("Should not return GOP on first frame")
	}

	// Add P-frame
	pframe := []byte{0x00, 0x00, 0x00, 0x01, 0x41, 0x9a, 0x21, 0x00}
	gop, complete = assembler.AddFrame(pframe, false)

	if complete {
		t.Error("P-frame should not complete a GOP")
	}

	// Add another I-frame (completes previous GOP)
	gop, complete = assembler.AddFrame(iframe, true)

	if !complete {
		t.Error("Second I-frame should complete previous GOP")
	}
	if gop == nil {
		t.Error("Should return completed GOP")
	}
	if len(gop.Frames) != 2 {
		t.Errorf("Expected 2 frames in GOP, got %d", len(gop.Frames))
	}
}
```

**Step 3: Run test (should fail)**

```bash
go test ./internal/recorder -v
```

Expected: FAIL - "undefined: NewGOPAssembler"

**Step 4: Implement GOP assembler**

Create `internal/recorder/gop_assembler.go`:

```go
package recorder

import (
	"sync"
	"time"
)

type Frame struct {
	Data      []byte
	Timestamp time.Time
	IsKeyFrame bool
}

type GOP struct {
	Frames    []Frame
	StartTime time.Time
	EndTime   time.Time
}

type GOPAssembler struct {
	maxSize      int
	currentGOP   []Frame
	gopStartTime time.Time
	mu           sync.Mutex
}

func NewGOPAssembler(maxSize int) *GOPAssembler {
	return &GOPAssembler{
		maxSize:    maxSize,
		currentGOP: make([]Frame, 0, 100),
	}
}

func (g *GOPAssembler) AddFrame(data []byte, isKeyFrame bool) (*GOP, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()

	now := time.Now()

	// If this is a key frame and we have existing frames, complete the GOP
	if isKeyFrame && len(g.currentGOP) > 0 {
		completedGOP := &GOP{
			Frames:    g.currentGOP,
			StartTime: g.gopStartTime,
			EndTime:   now,
		}

		// Start new GOP
		g.currentGOP = make([]Frame, 0, 100)
		g.gopStartTime = now
		g.currentGOP = append(g.currentGOP, Frame{
			Data:      append([]byte(nil), data...),
			Timestamp: now,
			IsKeyFrame: isKeyFrame,
		})

		return completedGOP, true
	}

	// Add frame to current GOP
	if len(g.currentGOP) == 0 {
		g.gopStartTime = now
	}

	g.currentGOP = append(g.currentGOP, Frame{
		Data:      append([]byte(nil), data...),
		Timestamp: now,
		IsKeyFrame: isKeyFrame,
	})

	return nil, false
}

func (g *GOPAssembler) isH264KeyFrame(nalu []byte) bool {
	if len(nalu) < 5 {
		return false
	}

	// Skip start code (0x00 0x00 0x00 0x01 or 0x00 0x00 0x01)
	offset := 0
	if len(nalu) >= 4 && nalu[0] == 0 && nalu[1] == 0 && nalu[2] == 0 && nalu[3] == 1 {
		offset = 4
	} else if len(nalu) >= 3 && nalu[0] == 0 && nalu[1] == 0 && nalu[2] == 1 {
		offset = 3
	}

	if offset >= len(nalu) {
		return false
	}

	naluType := nalu[offset] & 0x1F
	return naluType == 5 // IDR slice
}

func (g *GOPAssembler) Flush() *GOP {
	g.mu.Lock()
	defer g.mu.Unlock()

	if len(g.currentGOP) == 0 {
		return nil
	}

	gop := &GOP{
		Frames:    g.currentGOP,
		StartTime: g.gopStartTime,
		EndTime:   time.Now(),
	}

	g.currentGOP = make([]Frame, 0, 100)
	return gop
}
```

**Step 5: Run test (should pass)**

```bash
go test ./internal/recorder -v
```

Expected: PASS

**Step 6: Commit**

```bash
git add internal/recorder/ go.mod go.sum
git commit -m "feat: add GOP assembler for frame buffering"
```

---

### Task 6: fMP4 Builder (Simplified)

**Files:**
- Create: `pkg/fmp4/builder.go`
- Create: `pkg/fmp4/builder_test.go`

**Step 1: Add mp4ff dependency**

```bash
go get github.com/Eyevinn/mp4ff/mp4
```

**Step 2: Write test**

Create `pkg/fmp4/builder_test.go`:

```go
package fmp4

import (
	"testing"
)

func TestBuildInitSegment(t *testing.T) {
	sps := []byte{0x67, 0x42, 0x00, 0x1f, 0x8c, 0x8d, 0x40, 0x50}
	pps := []byte{0x68, 0xce, 0x3c, 0x80}

	data, err := BuildInitSegment("h264", sps, pps, nil)
	if err != nil {
		t.Fatalf("BuildInitSegment failed: %v", err)
	}

	if len(data) == 0 {
		t.Error("Expected non-empty init segment")
	}

	// Check for ftyp box signature
	if len(data) < 8 || string(data[4:8]) != "ftyp" {
		t.Error("Expected ftyp box at start")
	}
}
```

**Step 3: Run test (should fail)**

```bash
go test ./pkg/fmp4 -v
```

Expected: FAIL

**Step 4: Implement simplified fMP4 builder**

Create `pkg/fmp4/builder.go`:

```go
package fmp4

import (
	"bytes"
	"fmt"

	"github.com/Eyevinn/mp4ff/mp4"
)

func BuildInitSegment(codec string, sps, pps, vps []byte) ([]byte, error) {
	init := mp4.CreateEmptyInit()
	init.Moov.Mvhd.NextTrackID = 2

	// Create video track
	trak := &mp4.TrakBox{}
	trak.Tkhd = &mp4.TkhdBox{
		TrackID: 1,
		Width:   1920 << 16,
		Height:  1080 << 16,
	}

	// Simplified - would need proper implementation with actual codec data
	buf := bytes.Buffer{}
	err := init.Encode(&buf)
	if err != nil {
		return nil, fmt.Errorf("encode init: %w", err)
	}

	return buf.Bytes(), nil
}

func BuildFragment(frames [][]byte, sequenceNumber uint32) ([]byte, error) {
	// Simplified implementation
	// Real implementation would build moof + mdat boxes
	return nil, fmt.Errorf("not implemented")
}
```

**Step 5: Run test (should pass)**

```bash
go test ./pkg/fmp4 -v
```

Expected: PASS

**Step 6: Commit**

```bash
git add pkg/fmp4/ go.mod go.sum
git commit -m "feat: add simplified fMP4 builder (init segment)"
```

---

## Phase 2: Basic Playback (Weeks 4-6)

### Task 7: RTSP Playback Server Setup

**Files:**
- Create: `internal/playback/server.go`
- Create: `internal/playback/server_test.go`

**Step 1: Add gortsplib dependency**

```bash
go get github.com/bluenviron/gortsplib/v4
```

**Step 2: Implement basic RTSP server**

Create `internal/playback/server.go`:

```go
package playback

import (
	"fmt"

	"github.com/bluenviron/gortsplib/v4"
	"github.com/bluenviron/gortsplib/v4/pkg/base"
)

type Server struct {
	rtspServer *gortsplib.Server
	port       int
}

func NewServer(port int) *Server {
	return &Server{
		port: port,
	}
}

func (s *Server) Start() error {
	s.rtspServer = &gortsplib.Server{
		Handler: s,
		RTSPAddress: fmt.Sprintf(":%d", s.port),
	}

	return s.rtspServer.StartAndWait()
}

func (s *Server) OnDescribe(ctx *gortsplib.ServerHandlerOnDescribeCtx) (*base.Response, *gortsplib.ServerStream, error) {
	// Will implement in next task
	return nil, nil, fmt.Errorf("not implemented")
}

func (s *Server) Close() error {
	if s.rtspServer != nil {
		s.rtspServer.Close()
	}
	return nil
}
```

**Step 3: Commit**

```bash
git add internal/playback/ go.mod go.sum
git commit -m "feat: add basic RTSP playback server structure"
```

---

## Phase 3-5: Abbreviated Tasks

### Phase 3: Enhancement (Weeks 7-8)
- Task 8: Implement SeaweedFS hybrid storage (gRPC + HTTP)
- Task 9: Add fid pool and caching layers
- Task 10: Implement seek functionality (Range header parsing)
- Task 11: Implement speed control (Scale header parsing)
- Task 12: Add GOP prefetching

### Phase 4: Robustness (Weeks 9-10)
- Task 13: Implement memory controller with backpressure
- Task 14: Add recording state machine
- Task 15: Implement codec switching detection
- Task 16: Add reconnection logic
- Task 17: Implement scheduled recording

### Phase 5: Operations (Weeks 11-12)
- Task 18: Add Prometheus metrics
- Task 19: Implement REST API endpoints
- Task 20: Add config hot reload
- Task 21: Implement index snapshots
- Task 22: Create Docker Compose setup

---

## Execution Instructions

**For Claude executing this plan:**

1. Use `superpowers:executing-plans` skill
2. Execute tasks sequentially (Task 1 → Task 2 → ...)
3. Follow TDD: Write test → Run (fail) → Implement → Run (pass) → Commit
4. Run all tests before each commit: `go test ./... -v`
5. After Phase 1 completion, validate with integration test
6. Proceed to Phase 2 only after Phase 1 is stable

**Validation checkpoints:**
- After Task 6: Can assemble GOPs and store to SeaweedFS
- After Task 7: RTSP server accepts connections
- After Phase 2: End-to-end record and playback works
- After Phase 5: Full system passes 24h stability test

---

