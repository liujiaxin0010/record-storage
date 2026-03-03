# RTSP Recording & Playback System - Implementation Design

> **Project**: RecordingServer
> **Approach**: Sequential Phase Implementation
> **Timeline**: 8-12 weeks
> **Design Version**: 1.0
> **Date**: 2026-03-03

---

## 1. Executive Summary

This document defines the implementation design for a high-performance RTSP recording and playback system built in pure Go. The system will:

- Record 500-1000 concurrent 1080P RTSP streams to SeaweedFS distributed storage
- Provide RTSP-based playback with seek and speed control
- Maintain ≤6MB memory per stream through GOP-level flushing
- Achieve <200ms seek latency and <3ms write latency (hybrid mode)

**Implementation Strategy**: Sequential phase development over 5 phases, starting with basic recording and progressively adding playback, optimization, robustness, and operational features.

---

## 2. Architecture Overview

### 2.1 System Components

```
┌─────────────────────────────────────────────────────────────┐
│                    Recording Server (Go)                     │
│                                                              │
│  ┌──────────────────────────────────────────────────────┐   │
│  │ Puller Layer (RTSP Clients)                          │   │
│  │  - gortsplib-based RTSP client per camera            │   │
│  │  - RTP packet reception & NALU extraction            │   │
│  └────────────────────┬─────────────────────────────────┘   │
│                       │ NALUs                                │
│  ┌────────────────────▼─────────────────────────────────┐   │
│  │ Recorder Layer                                        │   │
│  │  - GOP Assembly (I/P/B frame buffering)              │   │
│  │  - Codec Detection (H264/H265/MPEG4)                 │   │
│  │  - fMP4 Fragment Building                            │   │
│  │  - State Machine (Idle/Recording/Stopping)           │   │
│  └────────────────────┬─────────────────────────────────┘   │
│                       │ fMP4 Fragments                       │
│  ┌────────────────────▼─────────────────────────────────┐   │
│  │ Async Writer (Queue + Workers)                       │   │
│  └────────┬──────────────────────────┬──────────────────┘   │
│           │                          │                       │
│  ┌────────▼────────┐        ┌────────▼────────┐            │
│  │ SeaweedFS       │        │ BoltDB Index    │            │
│  │ (Hybrid/HTTP)   │        │ (GOP Metadata)  │            │
│  └─────────────────┘        └─────────────────┘            │
│                                                              │
│  ┌──────────────────────────────────────────────────────┐   │
│  │ Playback Layer (RTSP Server)                         │   │
│  │  - Session Manager (per-client state)                │   │
│  │  - Fragment Reader (fMP4 → samples)                  │   │
│  │  - RTP Sender (NALU → RTP packets)                   │   │
│  │  - Prefetcher (GOP preloading)                       │   │
│  └──────────────────────────────────────────────────────┘   │
│                                                              │
│  ┌──────────────────────────────────────────────────────┐   │
│  │ Control Plane                                         │   │
│  │  - Memory Controller (backpressure)                  │   │
│  │  - REST API (queries, control)                       │   │
│  │  - Metrics (Prometheus)                              │   │
│  └──────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
```

### 2.2 Data Flow

**Recording Path** (zero disk I/O):
```
Camera RTSP → RTP Packets → NALUs → GOP Buffer (memory)
→ fMP4 Fragment (memory) → Async Queue → SeaweedFS + BoltDB Index
```

**Playback Path**:
```
Client PLAY Request → BoltDB Query (<1ms) → SeaweedFS GetRange
→ fMP4 Parse → NALUs → RTP Packets → Client
```

---

## 3. Project Structure

```
recording-server/
├── cmd/
│   └── server/
│       └── main.go                      # Entry point, dependency wiring
│
├── internal/
│   ├── config/
│   │   ├── config.go                    # Config structs
│   │   └── validator.go                 # Validation logic
│   │
│   ├── puller/
│   │   ├── manager.go                   # Multi-camera lifecycle management
│   │   └── rtsp_puller.go              # Single RTSP client (gortsplib)
│   │
│   ├── recorder/
│   │   ├── manager.go                   # Recording coordinator
│   │   ├── stream_recorder.go          # Per-camera state machine
│   │   ├── gop_assembler.go            # NALU → GOP buffering
│   │   ├── codec_detector.go           # H264/H265/MPEG4 detection
│   │   ├── fmp4_builder.go             # init + fragment generation
│   │   ├── async_writer.go             # Write queue + worker pool
│   │   └── chunk_writer.go             # Optional GOP merging
│   │
│   ├── storage/
│   │   ├── interface.go                 # StorageBackend abstraction
│   │   ├── seaweed_filer_http.go       # Phase 1: Simple HTTP PUT/GET
│   │   ├── seaweed_hybrid.go           # Phase 3: gRPC control + HTTP data
│   │   ├── grpc_master_client.go       # Master.Assign + LookupVolume
│   │   ├── grpc_filer_client.go        # CreateEntry + Lookup + Delete
│   │   ├── http_volume_client.go       # Volume PUT/GET/Range
│   │   ├── fid_pool.go                 # Batch fid pre-allocation
│   │   ├── volume_cache.go             # Volume routing cache
│   │   ├── entry_cache.go              # Entry metadata LRU cache
│   │   └── compensation_queue.go       # CreateEntry retry mechanism
│   │
│   ├── index/
│   │   ├── interface.go                 # IndexStore abstraction
│   │   ├── bolt_index.go               # BoltDB implementation
│   │   ├── codec.go                    # GOPMeta binary encoding
│   │   └── models.go                   # Data structures
│   │
│   ├── playback/
│   │   ├── server.go                   # RTSP server (gortsplib)
│   │   ├── handler.go                  # DESCRIBE/SETUP/PLAY/PAUSE/TEARDOWN
│   │   ├── session.go                  # Per-client state + seek/scale
│   │   ├── fragment_reader.go          # fMP4 → sample iterator
│   │   ├── rtp_sender.go              # NALU → RTP packet generation
│   │   └── prefetcher.go              # GOP preloading logic
│   │
│   ├── memory/
│   │   └── controller.go               # Global backpressure manager
│   │
│   └── api/
│       ├── server.go                   # HTTP REST server
│       └── handlers.go                 # Endpoint implementations
│
├── pkg/
│   └── fmp4/
│       ├── init_builder.go             # ftyp + moov generation
│       ├── fragment_builder.go         # moof + mdat generation
│       └── fragment_parser.go          # Fragment → samples parsing
│
├── config.yaml                          # Runtime configuration
├── go.mod
├── go.sum
├── Dockerfile
├── docker-compose.yaml
└── README.md
```

---

## 4. Core Interfaces

### 4.1 Storage Abstraction

Allows seamless upgrade from Filer HTTP (Phase 1) to Hybrid mode (Phase 3):

```go
type StorageBackend interface {
    // Put stores data at the given key
    Put(ctx context.Context, key string, data []byte) error

    // Get retrieves entire object
    Get(ctx context.Context, key string) ([]byte, error)

    // GetRange retrieves partial object (for seek optimization)
    GetRange(ctx context.Context, key string, offset, length int64) ([]byte, error)

    // Delete removes object
    Delete(ctx context.Context, key string) error

    // List enumerates objects with prefix
    List(ctx context.Context, prefix string) ([]string, error)
}
```

**Implementations**:
- `SeaweedFilerHTTPStorage`: Phase 1, single HTTP call to Filer
- `SeaweedHybridStorage`: Phase 3, gRPC control + HTTP data

### 4.2 Index Abstraction

```go
type IndexStore interface {
    // WriteGOP records a GOP's metadata
    WriteGOP(ctx context.Context, cameraID string, meta GOPMeta) error

    // QueryRange finds GOPs in time range
    QueryRange(ctx context.Context, cameraID string, start, end time.Time) ([]GOPMeta, error)

    // WriteEvent records recording lifecycle events
    WriteEvent(ctx context.Context, cameraID string, event RecordingEvent) error

    // GetRecordingRanges returns continuous recording segments for a date
    GetRecordingRanges(ctx context.Context, cameraID string, date time.Time) ([]TimeRange, error)
}

type GOPMeta struct {
    CameraID     string
    StartTime    time.Time
    EndTime      time.Time
    ObjectKey    string    // SeaweedFS key
    InitKey      string    // fMP4 init segment key
    Size         int64
    FrameCount   int
    Codec        string    // "h264", "h265", "mpeg4"
}

type RecordingEvent struct {
    Type      string    // "start", "stop", "disconnect", "reconnect", "codec_change"
    Timestamp time.Time
    Metadata  map[string]string
}
```

### 4.3 Memory Control

```go
type MemoryController interface {
    // Allocate reserves memory quota
    Allocate(size int64) error

    // Release returns memory quota
    Release(size int64)

    // GetPressureLevel returns 0-3 (normal to critical)
    GetPressureLevel() int
}
```

Pressure levels trigger backpressure:
- Level 0 (<60%): Normal operation
- Level 1 (60-80%): Accelerate flush
- Level 2 (80-90%): Drop B-frames
- Level 3 (>90%): I-frames only

---

## 5. Phase-by-Phase Implementation

### Phase 1: Basic Recording (Weeks 1-3)

**Goal**: Record RTSP streams to SeaweedFS with BoltDB indexing.

**Modules to implement**:
1. `config`: YAML loading and validation
2. `puller`: RTSP client using gortsplib
3. `recorder/gop_assembler`: Buffer NALUs until next I-frame
4. `recorder/fmp4_builder`: Generate init segment + fragments
5. `recorder/async_writer`: Queue + worker pool
6. `storage/seaweed_filer_http`: Simple HTTP PUT to Filer
7. `index/bolt_index`: BoltDB write/query operations

**Deliverables**:
- Can record 1+ cameras continuously
- fMP4 fragments stored in SeaweedFS
- GOP metadata indexed in BoltDB
- Basic error handling and logging

**Testing**:
- Unit tests for GOP assembly and fMP4 generation
- Integration test: mock RTSP → storage → index
- Manual verification: inspect SeaweedFS objects and BoltDB entries

**Acceptance criteria**:
- 10 cameras recording for 1 hour without crashes
- Memory usage ≤100MB (10 cameras × 6MB + overhead)
- All GOPs indexed correctly

---

### Phase 2: Basic Playback (Weeks 4-6)

**Goal**: RTSP playback server with continuous streaming.

**Modules to implement**:
1. `playback/server`: RTSP server using gortsplib
2. `playback/handler`: DESCRIBE/SETUP/PLAY/TEARDOWN
3. `playback/session`: Per-client state management
4. `playback/fragment_reader`: Parse fMP4 → samples
5. `playback/rtp_sender`: NALU → RTP packet generation
6. `pkg/fmp4/fragment_parser`: fMP4 parsing utilities

**Deliverables**:
- RTSP URL: `rtsp://host:8554/playback/{camera_id}?start={ISO8601}`
- Continuous playback of recorded streams
- Seamless GOP-to-GOP transitions

**Testing**:
- Unit tests for fMP4 parsing and RTP packing
- Integration test: index → storage → RTP output
- Manual verification: VLC/FFplay playback

**Acceptance criteria**:
- 10 concurrent playback sessions without stuttering
- No visible artifacts at GOP boundaries
- Playback starts within 500ms of PLAY request

---

### Phase 3: Playback Enhancement (Weeks 7-8)

**Goal**: Optimize storage access and add seek/speed features.

**Storage upgrade**:
1. Implement `storage/seaweed_hybrid`: gRPC control + HTTP data
2. Add `fid_pool`: Batch Assign calls
3. Add `volume_cache` and `entry_cache`
4. Add `compensation_queue`: Retry CreateEntry failures

**Playback features**:
1. Seek support: Parse Range header, query index, jump to GOP
2. Speed control: Parse Scale header, adjust frame timing/filtering
3. Prefetching: Load next GOP at 70% progress

**Deliverables**:
- Seek latency <200ms (P95)
- Speed control: 1/8x to 16x
- Write latency <3ms (P95) with hybrid mode

**Testing**:
- Benchmark seek latency across 100 random seeks
- Validate speed control at all supported rates
- Stress test: 1000 cameras recording with hybrid storage

**Acceptance criteria**:
- Seek works accurately (±2 seconds)
- Speed changes don't require reconnection
- Hybrid mode reduces write latency by 40% vs Filer HTTP

---

### Phase 4: Robustness (Weeks 9-10)

**Goal**: Production-grade reliability and resource management.

**Features to implement**:
1. `memory/controller`: Backpressure with 4-level degradation
2. `recorder/stream_recorder`: Full state machine (Idle/WaitingKeyFrame/Recording/Stopping/Disconnected)
3. `recorder/codec_detector`: Runtime codec switching detection
4. `puller`: Reconnection logic with exponential backoff
5. Scheduled recording: Time-based start/stop

**Deliverables**:
- Memory usage stays within configured limits under load
- Automatic reconnection after network failures
- Graceful handling of camera codec changes
- Scheduled recording based on time windows

**Testing**:
- Stress test: 1000 cameras for 24 hours
- Fault injection: Kill cameras, SeaweedFS nodes, network delays
- Memory pressure simulation: Limit to 4GB, verify backpressure

**Acceptance criteria**:
- No OOM crashes under 1000 camera load
- Reconnection succeeds within 30 seconds
- Codec switching doesn't corrupt recordings
- Scheduled recording accuracy ±30 seconds

---

### Phase 5: Operations & Monitoring (Weeks 11-12)

**Goal**: Observability, management, and deployment.

**Features to implement**:
1. Prometheus metrics: recording/playback/storage/memory
2. REST API: `/api/v1/recordings/{camera_id}/ranges`, `/api/v1/system/stats`
3. Config hot reload: Watch config file changes
4. Index snapshots: Periodic backup to SeaweedFS
5. Docker Compose: Full stack deployment

**Deliverables**:
- Grafana dashboard with key metrics
- REST API for recording queries and control
- Docker Compose for single-command deployment
- Operational runbook

**Testing**:
- End-to-end integration tests
- 24-hour stability run with monitoring
- Deployment test: Docker Compose up → record → playback

**Acceptance criteria**:
- All metrics visible in Prometheus
- API response time <50ms (P95)
- Docker Compose deployment works on fresh system
- Index recovery from snapshot succeeds

---

## 6. Key Technical Decisions

### 6.1 Storage Evolution Path

**Decision**: Start with Filer HTTP, upgrade to Hybrid in Phase 3.

**Rationale**:
- Filer HTTP is simpler (single call), gets recording working faster
- Hybrid mode optimization can wait until playback demands low latency
- Storage interface abstraction makes upgrade seamless

**Implementation**:
- Phase 1: `SeaweedFilerHTTPStorage` with `PUT /path/to/object`
- Phase 3: `SeaweedHybridStorage` with `Master.Assign → Volume PUT → Filer.CreateEntry`

### 6.2 GOP Buffering Strategy

**Decision**: Use `sync.Pool` for 2MB GOP buffers, flush on I-frame detection.

**Rationale**:
- 2MB handles typical GOP size (2 seconds @ 4Mbps = ~1MB)
- `sync.Pool` reduces GC pressure for 1000 concurrent streams
- Flush on I-frame ensures clean GOP boundaries

**Implementation**:
```go
var gopBufferPool = sync.Pool{
    New: func() interface{} {
        return make([]byte, 0, 2*1024*1024)
    },
}
```

### 6.3 Index Schema

**Decision**: BoltDB with key `{camera_id}:{unix_nano}`, binary-encoded value.

**Rationale**:
- B+ tree naturally sorts by time
- Binary encoding (protobuf or custom) minimizes storage
- Batch writes every 100ms balance latency and throughput

**Schema**:
```
Bucket: "gop_index"
Key:    "cam01:1709481234567890123"
Value:  <binary GOPMeta>
```

### 6.4 Playback Session Isolation

**Decision**: Each RTSP session = independent goroutine, no shared state.

**Rationale**:
- Simplifies concurrency (no locks between sessions)
- Session-local prefetch cache prevents interference
- Easy to implement pause/resume per session

**Implementation**:
- `SessionManager` tracks active sessions
- Each `Session` has own goroutine for RTP sending
- Sessions communicate via channels

### 6.5 Error Handling Philosophy

**Decision**: Recording fails gracefully, playback fails fast, storage retries.

**Rationale**:
- Recording: Uptime > completeness (drop frames, don't crash)
- Playback: Correctness > availability (close session on corruption)
- Storage: Transient failures are common (retry with backoff)

**Implementation**:
- Recording: Log errors, continue with next GOP
- Playback: Return error to client, close session
- Storage: Retry up to 3 times with exponential backoff

---

## 7. Testing Strategy

### 7.1 Unit Tests

**Per-module tests**:
- `gop_assembler_test.go`: NALU → GOP, I-frame detection, buffer overflow
- `fmp4_builder_test.go`: Valid fMP4 structure (use mp4ff validator)
- `bolt_index_test.go`: Write/query correctness, concurrent access
- `codec_detector_test.go`: H264/H265 switching detection
- `rtp_sender_test.go`: NALU → RTP packet correctness

**Coverage target**: >80% for core logic

### 7.2 Integration Tests

**Cross-module tests**:
- `recording_flow_test.go`: Mock RTSP → GOP → fMP4 → storage → index
- `playback_flow_test.go`: Index query → storage read → RTP output
- `seek_test.go`: Seek accuracy and latency measurement
- `storage_hybrid_test.go`: gRPC+HTTP flow with mock SeaweedFS

### 7.3 End-to-End Tests

**Full system tests**:
- `e2e_record_playback_test.go`: Real RTSP source → record → playback via VLC
- `stress_test.go`: 100+ concurrent cameras, measure memory/CPU/latency
- `fault_injection_test.go`: SeaweedFS failures, network delays, camera disconnects

### 7.4 Performance Benchmarks

**Benchmarks**:
- `BenchmarkGOPAssembly`: Throughput (frames/sec)
- `BenchmarkFMP4Build`: Fragment generation time
- `BenchmarkIndexQuery`: Query latency (P50/P95/P99)
- `BenchmarkSeekLatency`: End-to-end seek time
- `BenchmarkStoragePut`: Write latency distribution

**Targets**:
- GOP assembly: >1000 frames/sec per stream
- Index query: <1ms (P95)
- Seek latency: <200ms (P95)
- Storage PUT: <3ms (P95, hybrid mode)

---

## 8. Dependencies

### 8.1 Core Libraries

```go
require (
    github.com/bluenviron/gortsplib/v4 v4.x.x    // RTSP client/server
    github.com/bluenviron/mediacommon v1.x.x     // H264/H265 parsing
    github.com/Eyevinn/mp4ff v0.x.x              // fMP4 handling
    github.com/pion/rtp v1.x.x                   // RTP packet handling
    go.etcd.io/bbolt v1.x.x                      // BoltDB
    google.golang.org/grpc v1.x.x                // gRPC client
    google.golang.org/protobuf v1.x.x            // Protobuf
    gopkg.in/yaml.v3 v3.x.x                      // Config parsing
    github.com/rs/zerolog v1.x.x                 // Logging
    github.com/prometheus/client_golang v1.x.x   // Metrics
)
```

### 8.2 SeaweedFS Proto

Generate Go code from SeaweedFS proto files:
```bash
protoc --go_out=. --go-grpc_out=. \
    weed/pb/master.proto \
    weed/pb/filer.proto \
    weed/pb/volume_server.proto
```

Place generated code in `internal/storage/pb/`.

---

## 9. Configuration

### 9.1 Config File Structure

```yaml
server:
  rtsp_playback_port: 8554
  api_port: 8080
  metrics_port: 9090

memory:
  max_memory_mb: 4096
  gop_buffer_pool_size: 2097152

storage:
  seaweedfs:
    mode: "filer_http"  # Phase 1: filer_http, Phase 3: hybrid
    filer_http_url: "http://seaweedfs-filer:8888"
    master_grpc_addrs: ["seaweedfs-master:19333"]  # Phase 3
    filer_grpc_addrs: ["seaweedfs-filer:18888"]    # Phase 3

index:
  bolt_path: "/data/index/recordings.db"
  batch_interval_ms: 100

recorder:
  segment_mode: "gop"
  async_writer_workers: 8
  async_writer_queue_size: 1000
  retry_max: 3

cameras:
  - id: "cam01"
    rtsp_url: "rtsp://admin:pass@192.168.1.64:554/Streaming/Channels/101"
    transport: "tcp"
    enabled: true
    recording:
      auto_start: true
```

---

## 10. Deployment

### 10.1 Docker Compose

```yaml
version: '3.8'

services:
  recording-server:
    build: .
    ports:
      - "8554:8554"  # RTSP playback
      - "8080:8080"  # REST API
      - "9090:9090"  # Metrics
    volumes:
      - ./config.yaml:/app/config.yaml
      - index-data:/data/index
    depends_on:
      - seaweedfs-master
      - seaweedfs-filer

  seaweedfs-master:
    image: chrislusf/seaweedfs:latest
    command: "master -ip=seaweedfs-master"
    ports:
      - "9333:9333"
      - "19333:19333"

  seaweedfs-volume:
    image: chrislusf/seaweedfs:latest
    command: "volume -mserver=seaweedfs-master:9333 -port=8080"
    ports:
      - "8081:8080"
      - "18080:18080"
    volumes:
      - volume-data:/data

  seaweedfs-filer:
    image: chrislusf/seaweedfs:latest
    command: "filer -master=seaweedfs-master:9333"
    ports:
      - "8888:8888"
      - "18888:18888"
    depends_on:
      - seaweedfs-master

volumes:
  index-data:
  volume-data:
```

---

## 11. Success Metrics

### 11.1 Functional Metrics

- ✅ Record 500+ cameras simultaneously
- ✅ Playback with <500ms startup time
- ✅ Seek accuracy ±2 seconds
- ✅ Speed control 1/8x to 16x
- ✅ Automatic reconnection after failures

### 11.2 Performance Metrics

- ✅ Memory: ≤6MB per recording stream
- ✅ Write latency: <3ms (P95, hybrid mode)
- ✅ Read latency: <20ms (P95)
- ✅ Seek latency: <200ms (P95)
- ✅ Index query: <1ms (P95)

### 11.3 Reliability Metrics

- ✅ 24-hour stability test without crashes
- ✅ Reconnection success rate >99%
- ✅ Data loss ≤1 GOP per failure
- ✅ Memory backpressure prevents OOM

---

## 12. Next Steps

After design approval:

1. **Initialize Go module**: `go mod init recording-server`
2. **Create project structure**: Directories and placeholder files
3. **Phase 1 implementation**: Start with config → puller → recorder → storage → index
4. **Iterative development**: Build, test, refine each module
5. **Phase transitions**: Complete each phase before moving to next

**Estimated timeline**: 8-12 weeks for full system implementation.
