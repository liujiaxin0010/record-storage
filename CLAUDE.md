# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

High-performance RTSP video recording and playback system designed for 500+ concurrent camera streams. Records video from IP cameras to distributed storage (SeaweedFS) and provides RTSP-based playback with seek/speed control.

**Key Design Principle**: Zero-disk-IO recording path - all video data flows through memory directly to network storage, never touching local disk.

## Essential Commands

### Build and Run
```bash
# Build all packages
go build ./...

# Run server with default config
go run cmd/server/main.go

# Run with custom config
go run cmd/server/main.go path/to/config.yaml

# Validate config without starting
go run cmd/server/main.go --validate config.yaml

# Run index migration tool (v1 string keys → v2 binary keys)
go run cmd/migrate/main.go
```

### Testing
```bash
# Run all tests
go test ./...

# Run tests with coverage
go test -cover ./...

# Run specific package tests
go test ./internal/recorder/...

# Run single test
go test -run TestGOPAssembler ./internal/recorder/

# Benchmark tests (important for performance validation)
go test -bench=. -benchmem ./internal/recorder/
go test -bench=BenchmarkKeyEncoding ./internal/index/
```

### Development
```bash
# Install dependencies
go mod download

# Update dependencies
go get -u ./...
go mod tidy

# Generate protobuf code (if .proto files change)
# Note: pb/ directory contains SeaweedFS gRPC client code
protoc --go_out=. --go-grpc_out=. pb/**/*.proto
```

## Architecture Overview

### Data Flow: Recording Path

```
IPC Camera (RTSP)
  → RTSPPuller (收流层)
    → StreamRecorder (录制层)
      → GOPAssembler (GOP组装)
        → FMP4Builder (fMP4封装)
          → AsyncWriter (异步写入队列)
            ├→ SeaweedFS (分布式存储)
            └→ BoltDB (索引)
```

**Critical Path Details**:
1. **RTSPPuller** (`internal/puller/rtsp_puller.go`): One goroutine per camera, pulls RTP packets, decodes to NAL units
2. **StreamRecorder** (`internal/recorder/stream_recorder.go`): State machine (Idle → WaitingKeyFrame → Recording → Stopping), manages recording lifecycle
3. **GOPAssembler** (`internal/recorder/gop_assembler.go`): Accumulates frames until GOP complete (I-frame detected), triggers flush
4. **FMP4Builder** (`internal/recorder/fmp4_builder.go`): Encapsulates GOP into fragmented MP4 format (init segment + media fragment)
5. **AsyncWriter** (`internal/recorder/async_writer.go`): Decouples recording from storage I/O, 8 worker goroutines, 1000-deep queue with backpressure

### Data Flow: Playback Path

```
RTSP Client Request
  → PlaybackServer (RTSP信令)
    → BoltDB Index Query (<1ms)
      → SeaweedFS GET/Range
        → FragmentReader (fMP4解析)
          → RTPSender (RTP打包)
            → RTSP Client
```

**Playback Optimizations**:
- **Prefetcher** (`internal/playback/prefetcher.go`): Async prefetch next GOP when current reaches 70% progress, LRU cache max 10 GOPs
- **Seek**: Binary search in BoltDB index by timestamp, jump to nearest I-frame
- **Speed Control**: Scale 1-16x, high speeds drop B/P frames, only send I-frames

### Storage Architecture

**SeaweedFS Hybrid Mode** (default for production):
- **Control Plane**: gRPC to Master (Assign fid) + Filer (CreateEntry metadata)
- **Data Plane**: HTTP PUT/GET directly to Volume servers
- **Why**: Reduces latency vs pure Filer HTTP path, avoids FUSE overhead

**Object Layout**:
```
/recordings/{camera_id}/
  ├── init/{codec_hash}.mp4init    # fMP4 init segment (~500B, reused per codec)
  └── data/{YYYY-MM-DD}/
      ├── {unix_ms}.frag           # Each GOP = 1 object (0.5-2MB)
      └── ...
```

**Index Structure** (BoltDB):
- **V2 Binary Keys** (17 bytes): `[version:1][camera_hash:8][timestamp:8]` - 5x faster than v1 string keys
- **Bucket Hierarchy**: `camera → date → GOPs` for efficient range queries
- **Value**: JSON-encoded GOPMeta (ObjectKey, InitKey, StartTime, EndTime, Size, FrameCount, Codec)

## Key Technical Constraints

### Memory Management
- **Target**: 6MB per camera stream (500 cameras = 3GB base + 3.6GB overhead = 6.6GB total)
- **MemoryController** (`internal/memory/controller.go`): 4-level backpressure (0: normal, 1: accelerate flush, 2: drop B-frames, 3: I-frames only)
- **GOP Buffer Pool**: Use `sync.Pool` to reduce allocations (critical for 500-camera scale)

### Concurrency Patterns
- **Lock-Free Hot Path**: `StreamRecorder.ProcessSample()` uses `atomic.Value` for state, only locks on builder rebuild
- **Batch Writes**: BoltDB index writes batched (100ms or 50 entries) to reduce transaction overhead
- **Dynamic Worker Pool**: AsyncWriter scales 4-16 workers based on queue depth

### Error Handling Rules
1. **Never Silent Fail**: All errors must be logged or propagated (see requirements-analysis.md §12 risk matrix)
2. **Backpressure Over Drop**: AsyncWriter queue full → block caller (configurable: block/drop/drop_oldest)
3. **Exponential Backoff**: RTSPPuller reconnection 1s→30s, storage retries 50ms→1s (max 3 attempts)

## Critical Code Patterns

### Codec Detection and Runtime Switching
Cameras may change codec (H.264 ↔ H.265) without disconnecting. Detection logic in `internal/recorder/gop_assembler.go`:
- H.264: 1-byte NALU header `[type:5bits]`
- H.265: 2-byte NALU header `[type:6bits]`
- On detection: flush current GOP → generate new init segment → wait for I-frame → resume

### Fragmented MP4 Format
- **init segment** (ftyp + moov): Codec parameters only, stored once per codec config
- **media fragment** (moof + mdat): One complete GOP, self-contained with timing info
- **Crash Safety**: Only lose last incomplete fragment (~2 seconds), all prior GOPs already in SeaweedFS

### Index Migration (V1 → V2)
When upgrading to binary keys, use dual-write mode:
1. Deploy with both v1 and v2 writes enabled
2. Run `cmd/migrate/main.go` to backfill historical data
3. Switch reads to v2 after validation
4. Remove v1 keys after retention period

## Monitoring and Observability

**Prometheus Metrics** (`:9090/metrics`):
- `recording_streams_active`: Current recording camera count
- `playback_sessions_active`: Active RTSP playback sessions
- `gops_written_total`: Total GOPs successfully written
- `storage_write_latency_seconds`: P50/P95/P99 write latency (target: P95 < 3ms)
- `writer_queue_depth`: AsyncWriter queue length (alert if > 800)

**REST API** (`:8080/api/v1/`):
- `GET /recordings/{camera_id}?date=2024-01-01`: Query recording time ranges
- `GET /system/stats`: System health (active recorders, queue stats)

**RTSP Playback** (`:8554`):
```
rtsp://localhost:8554/playback/{camera_id}?start=2024-01-01T08:00:00Z&end=2024-01-01T20:00:00Z
```

## Configuration Notes

**config.yaml** key settings:
- `storage.seaweedfs.mode`: Use `"hybrid"` for production (gRPC+HTTP), `"filer_http"` for simple deployments
- `recorder.async_writer_workers`: Scale 4-16 based on camera count (1 worker per ~60 cameras)
- `recorder.async_writer_queue_size`: 1000 default, increase if `writer_queue_depth` metric shows saturation
- `memory.max_memory_mb`: Set to 80% of available RAM to leave headroom for OS/GC

## Common Pitfalls

1. **Don't modify samples after passing to recorder**: `ProcessSample()` may retain references for async processing
2. **Index queries must use correct bucket**: V2 keys require date-based bucket navigation
3. **SeaweedFS fid format**: `{volume_id},{needle_id},{cookie}` - parse volume_id before HTTP requests
4. **RTSP transport mode**: Use `"tcp"` for reliable recording, `"udp"` only for low-latency preview
5. **GOP boundary detection**: Must wait for I-frame before starting recording, partial GOPs are invalid

## Performance Targets

| Metric | Target | Validation |
|--------|--------|------------|
| Single camera memory | ≤ 6MB | Monitor `MemoryController` pressure level |
| Write latency (P95) | < 3ms | Check `storage_write_latency_seconds` |
| Index query | < 1ms | Benchmark `BenchmarkIndexWrite` |
| Seek response | < 200ms | End-to-end playback test |
| Crash data loss | ≤ 2 seconds | Last incomplete GOP only |

## Related Documentation

- `requirements-analysis.md`: Full system requirements and design rationale
- `docs/plans/implementation-design.md`: Detailed implementation specifications
- `docs/plans/2026-03-03-rtsp-recording-playback.md`: Original design document
