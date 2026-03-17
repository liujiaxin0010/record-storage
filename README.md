# RecordingServer — 高性能 RTSP 录像存储与回放系统

纯 Go 技术栈的 RTSP 录像存储与回放服务，专注于高性能写入、分布式存储和低延迟回放。设计目标为单节点支持 **500-1000 路 1080P@25fps** 摄像头并发录制，全内存数据通路实现 **零磁盘 IO** 录制。

---

## 目录

- [系统架构](#系统架构)
- [核心数据流](#核心数据流)
- [模块详解](#模块详解)
  - [收流层 — RTSPPuller](#收流层--rtsppuller)
  - [录制层 — StreamRecorder](#录制层--streamrecorder)
  - [GOP 组装器 — GOPAssembler](#gop-组装器--gopassembler)
  - [fMP4 封装器 — FMP4Builder](#fmp4-封装器--fmp4builder)
  - [异步写入层 — AsyncWriter](#异步写入层--asyncwriter)
  - [存储层 — SeaweedFS 混合模式](#存储层--seaweedfs-混合模式)
  - [索引层 — BoltDB](#索引层--boltdb)
  - [回放层 — Playback Server](#回放层--playback-server)
  - [内存控制器 — MemoryController](#内存控制器--memorycontroller)
  - [录制调度器 — ScheduleController](#录制调度器--schedulecontroller)
  - [录像过期清理 — RetentionCleaner](#录像过期清理--retentioncleaner)
  - [索引快照 — SnapshotManager](#索引快照--snapshotmanager)
  - [REST API](#rest-api)
  - [监控指标 — Prometheus Metrics](#监控指标--prometheus-metrics)
- [录制控制状态机](#录制控制状态机)
- [存储对象结构](#存储对象结构)
- [索引 Key 设计](#索引-key-设计)
- [内存背压策略](#内存背压策略)
- [倍速播放与抽帧策略](#倍速播放与抽帧策略)
- [SeaweedFS 写入事务与补偿](#seaweedfs-写入事务与补偿)
- [降级与容错](#降级与容错)
- [项目结构](#项目结构)
- [技术选型](#技术选型)
- [配置说明](#配置说明)
- [快速开始](#快速开始)
- [开发与测试](#开发与测试)
- [性能目标](#性能目标)

---

## 系统架构

```mermaid
graph TB
    subgraph 收流层
        CAM1[IPC 摄像头 1] -->|RTSP/RTP| P1[RTSPPuller]
        CAM2[IPC 摄像头 2] -->|RTSP/RTP| P2[RTSPPuller]
        CAM3[IPC 摄像头 N] -->|RTSP/RTP| P3[RTSPPuller]
    end

    subgraph 录制层
        P1 -->|Sample 回调| SR1[StreamRecorder]
        P2 -->|Sample 回调| SR2[StreamRecorder]
        P3 -->|Sample 回调| SR3[StreamRecorder]
        SR1 --> GA1[GOPAssembler]
        SR2 --> GA2[GOPAssembler]
        SR3 --> GA3[GOPAssembler]
        GA1 --> FB1[FMP4Builder]
        GA2 --> FB2[FMP4Builder]
        GA3 --> FB3[FMP4Builder]
    end

    subgraph 异步写入层
        FB1 --> AW[AsyncWriter<br/>N Workers / 队列 1000]
        FB2 --> AW
        FB3 --> AW
    end

    subgraph 存储与索引
        AW -->|gRPC Assign + HTTP PUT<br/>+ gRPC CreateEntry| SW[(SeaweedFS<br/>分布式存储)]
        AW -->|BatchWriteGOP| BI[(BoltDB<br/>录像索引)]
    end

    subgraph 回放层
        CLIENT[RTSP 客户端] -->|DESCRIBE/SETUP/PLAY| PBS[Playback Server]
        PBS -->|QueryRange| BI
        PBS -->|HTTP GET/Range| SW
        PBS -->|fMP4 解析 → RTP| CLIENT
    end

    subgraph 全局控制
        MC[MemoryController<br/>四级背压]
        SC[ScheduleController<br/>计划任务]
        RC[RetentionCleaner<br/>过期删除]
        SM[SnapshotManager<br/>索引备份]
        PROM[Prometheus<br/>:9090]
        API[REST API<br/>:8080]
    end

    MC -.->|背压信号| SR1
    MC -.->|内存配额| AW
    SC -.->|Start/Stop| SR1
    RC -.->|Delete| SW
    RC -.->|DeleteRange| BI
    SM -.->|Snapshot| BI
    SM -.->|Upload| SW
```

---

## 核心数据流

### 录制数据流（零磁盘 IO）

```mermaid
sequenceDiagram
    participant CAM as IPC 摄像头
    participant PULLER as RTSPPuller
    participant REC as StreamRecorder
    participant ASM as GOPAssembler
    participant BLD as FMP4Builder
    participant AW as AsyncWriter
    participant MASTER as SeaweedFS Master
    participant VOL as SeaweedFS Volume
    participant FILER as SeaweedFS Filer
    participant IDX as BoltDB Index

    CAM->>PULLER: RTSP/RTP 视频流
    PULLER->>REC: Sample (NALUs + 编码参数)
    REC->>REC: 内存背压检查 + 编码切换检测
    REC->>ASM: AddSample()

    Note over ASM: 累积帧直到检测到下一个 I 帧

    ASM-->>REC: flush=true (GOP 完成)
    REC->>ASM: Flush() → []Sample
    REC->>BLD: BuildFragment(samples)
    BLD-->>REC: fMP4 fragment (moof+mdat)
    REC->>AW: Submit(WriteJob)

    Note over AW: 队列解耦, 批量处理最多 10 个 Job

    AW->>MASTER: gRPC Assign (获取 fid)
    MASTER-->>AW: fid + volumeURL
    AW->>VOL: HTTP PUT /{fid} (写入数据)
    VOL-->>AW: 201 Created
    AW->>FILER: gRPC CreateEntry (注册元数据)
    FILER-->>AW: OK
    AW->>IDX: BatchWriteGOP (写入索引)
```

### 回放数据流

```mermaid
sequenceDiagram
    participant CLIENT as RTSP 客户端
    participant PBS as Playback Server
    participant IDX as BoltDB Index
    participant FILER as SeaweedFS Filer
    participant VOL as SeaweedFS Volume
    participant PF as Prefetcher

    CLIENT->>PBS: DESCRIBE /playback/cam01?start=...
    PBS->>IDX: QueryRange (查找 GOP)
    IDX-->>PBS: GOPMeta 列表
    PBS->>VOL: GET init segment
    PBS-->>CLIENT: 200 OK (SDP)

    CLIENT->>PBS: SETUP (Transport: RTP/AVP/TCP)
    PBS-->>CLIENT: 200 OK

    CLIENT->>PBS: PLAY (Range: npt=0-, Scale: 1.0)

    loop 逐 GOP 播放
        PBS->>VOL: HTTP GET /{fid} (读取 GOP)
        VOL-->>PBS: fMP4 fragment
        PBS->>PBS: fMP4 解析 → Samples
        PBS->>PF: 异步预载下一个 GOP

        loop 逐帧发送
            PBS->>PBS: 倍速抽帧判断
            PBS->>PBS: RTP 打包 (RFC 6184/7798)
            PBS->>CLIENT: RTP 数据包
        end
    end

    CLIENT->>PBS: TEARDOWN
    PBS->>PBS: 释放 Session 资源
```

---

## 模块详解

### 收流层 — RTSPPuller

> 文件：`internal/puller/rtsp_puller.go`

每路摄像头独立一个协程，通过 gortsplib 作为 RTSP Client 拉取视频流。

```mermaid
stateDiagram-v2
    [*] --> Connecting: Start()
    Connecting --> Playing: DESCRIBE → SETUP → PLAY
    Playing --> Playing: 持续接收 RTP 包
    Playing --> Reconnecting: 连接断开
    Reconnecting --> Connecting: 指数退避 (1s→30s)
    Reconnecting --> [*]: Stop()
    Playing --> [*]: Stop()
```

**核心能力：**

| 特性 | 实现 |
|------|------|
| 传输协议 | TCP / UDP / Auto |
| 编码格式 | H.264 (AVC)、H.265 (HEVC)、MPEG-4 Part 2 |
| 编码检测 | RTSP DESCRIBE SDP 自动识别 |
| 断线重连 | 指数退避 1s→30s，自动恢复 |
| 编码参数跟踪 | 每帧检测 SPS/PPS/VPS 变化 |
| 事件回调 | connect / disconnect / reconnect / reconnect_failed |

**编码格式检测逻辑：**

- **H.264**：`NALU[0] & 0x1F` 提取 type，type=7 为 SPS，type=8 为 PPS，type=5 为 IDR（I 帧）
- **H.265**：`(NALU[0] >> 1) & 0x3F` 提取 type，type=32 为 VPS，type=33 为 SPS，type=34 为 PPS，type=19/20 为 IDR
- **MPEG-4**：检测 GroupOfVOP 起始码 `0x000001B3`

---

### 录制层 — StreamRecorder

> 文件：`internal/recorder/stream_recorder.go`

单路录制器，管理从收流到写入的完整生命周期，包含状态机、编码切换检测和内存背压集成。

**关键设计：**

- **热路径无锁**：`ProcessSample()` 通过 `atomic.Value` 读取状态，仅在 builder 重建时加锁
- **编码动态切换**：检测 codec/params 变化 → flush 当前 GOP → 生成新 init segment → 等待 I 帧
- **内存背压集成**：通过 `MemoryController.ShouldDropFrame()` 在高压力下丢弃 B/P 帧

```go
// 热路径：每帧调用，lock-free 状态检查
func (s *StreamRecorder) ProcessSample(sample Sample) error {
    state := s.state.Load().(RecorderState) // atomic, no lock
    if state == RecorderStateIdle || state == RecorderStateStopping {
        return nil
    }
    // 内存背压：Level 2 丢 B 帧，Level 3 只保留 I 帧
    if s.memCtrl != nil && !sample.IsKey && s.memCtrl.ShouldDropFrame(string(sample.FrameType)) {
        return nil
    }
    // ... 编码切换检测 + GOP 组装 + 提交写入
}
```

---

### GOP 组装器 — GOPAssembler

> 文件：`internal/recorder/gop_assembler.go`

将连续的视频帧累积为完整的 GOP（Group of Pictures），以 I 帧为边界触发 flush。

```mermaid
graph LR
    subgraph GOP 组装过程
        I1[I 帧] --> P1[P 帧] --> P2[P 帧] --> B1[B 帧] --> I2[I 帧]
    end
    I2 -->|flush=true| FLUSH[Flush 上一个 GOP]
    FLUSH --> OUTPUT[返回 samples + startTime]
    I2 -->|重新开始累积| NEXT[新 GOP]
```

**组装规则：**
1. 等待首个 I 帧才开始累积（丢弃之前的 P/B 帧）
2. 当检测到下一个 I 帧时，返回 `flush=true` 信号
3. 调用方执行 `Flush()` 获取已累积的完整 GOP
4. 将触发 flush 的 I 帧重新 `AddSample()` 作为新 GOP 的起始
5. 所有 Sample 通过 `copySample()` 深拷贝，避免引用被外部修改

---

### fMP4 封装器 — FMP4Builder

> 文件：`internal/recorder/fmp4_builder.go`

基于 [Eyevinn/mp4ff](https://github.com/Eyevinn/mp4ff) 库，将 GOP 封装为标准的 Fragmented MP4 格式。

**输出结构：**

| 组件 | Box 结构 | 大小 | 存储策略 |
|------|---------|------|---------|
| init segment | `ftyp` + `moov` + `rinf`(自定义元数据) | ~500B | 编码参数不变时只存一份，key 为 SHA1 hash |
| media fragment | `moof` + `mdat` | 0.5-2MB | 每个 GOP 一个独立对象 |

**Sample 编码格式：**
- H.264/H.265：4 字节长度前缀 (big-endian) + NALU 数据
- MPEG-4：原始字节流

**时间戳计算：**
- Timescale = 90000（标准 RTP 时钟频率）
- duration = `(sample[i+1].Timestamp - sample[i].Timestamp) * 90000 / 1e9`
- 最后一帧继承前一帧的 duration
- 零/负 duration 回退为 3600（即 40ms@90kHz）

---

### 异步写入层 — AsyncWriter

> 文件：`internal/recorder/async_writer.go`

解耦录制与存储 IO，通过 worker 池和有界队列实现异步批量写入。

```mermaid
graph TB
    subgraph 提交端
        SR1[StreamRecorder 1] -->|Submit| Q[WriteJob Channel<br/>cap=1000]
        SR2[StreamRecorder 2] -->|Submit| Q
        SR3[StreamRecorder N] -->|Submit| Q
    end

    subgraph Worker 池
        Q --> W1[Worker 1]
        Q --> W2[Worker 2]
        Q --> W3[Worker N]
    end

    subgraph 批处理
        W1 -->|batch ≤ 10| BATCH[processBatch]
        BATCH -->|Put init + gop| STORE[(Storage)]
        BATCH -->|BatchWriteGOP| INDEX[(Index)]
    end

    subgraph 背压策略
        FULL{队列满?}
        FULL -->|Block| WAIT[阻塞等待]
        FULL -->|Drop| DROP[丢弃新 Job]
        FULL -->|DropOldest| EVICT[淘汰最旧 Job]
    end
```

**背压策略：**

| 策略 | 行为 | 适用场景 |
|------|------|---------|
| `block` | 队列满时阻塞调用方 | 默认策略，保证数据不丢 |
| `drop` | 队列满时丢弃新 Job | 高并发、容忍少量丢失 |
| `drop_oldest` | 淘汰队列头部最旧 Job | 优先保留最新数据 |

**内存跟踪：**
- Submit 时：`MemoryController.Allocate(jobSize)` 预留内存配额
- Worker 处理完成后：`MemoryController.Release(jobSize)` 释放配额
- 分配失败时直接丢弃 Job，防止 OOM

---

### 存储层 — SeaweedFS 混合模式

> 文件：`internal/storage/seaweed_hybrid.go`、`seaweed_filer_http.go`、`grpc_master_client.go`、`grpc_filer_client.go`、`http_volume_client.go`

采用 **gRPC 控制面 + HTTP 数据面** 混合模式，兼顾低延迟和高吞吐。

```mermaid
graph LR
    subgraph 控制面 gRPC
        MASTER[Master<br/>:19333]
        FILER[Filer<br/>:18888]
    end

    subgraph 数据面 HTTP
        VOL1[Volume 1<br/>:8080]
        VOL2[Volume 2<br/>:8080]
        VOL3[Volume N<br/>:8080]
    end

    subgraph 录制服务
        HYB[SeaweedHybridStorage]
        FB[FilerHTTPStorage<br/>降级回退]
    end

    HYB -->|1. Assign fid| MASTER
    HYB -->|2. PUT data| VOL1
    HYB -->|3. CreateEntry| FILER
    HYB -.->|降级| FB
    FB -->|HTTP PUT/GET| FILER
```

**写入三段式链路：**

```
1. Master.Assign (gRPC, 300ms 超时)  → 获取 fid + volumeURL
2. Volume PUT    (HTTP,  2s 超时)    → 直写数据到 Volume Server
3. Filer.CreateEntry (gRPC, 300ms)   → 注册路径元数据
```

**缓存层：**

| 缓存 | Key | TTL | 用途 |
|------|-----|-----|------|
| entryCache | object_key | 120s | 避免重复 LookupEntry |
| volumeCache | volume_id | 300s | 避免重复 LookupVolume |

**重试策略：**
- 最多 3 次重试
- 指数退避：50ms → 100ms → 200ms（上限 1s）
- 不可重试错误：`NotFound`、`InvalidFID`、`context.Canceled`

---

### 索引层 — BoltDB

> 文件：`internal/index/bolt_index.go`、`interface.go`、`snapshot.go`

嵌入式 B+ 树 KV 数据库，支持 V1（字符串键）和 V2（二进制键）双写模式。

```mermaid
graph TB
    subgraph BoltDB Buckets
        ROOT_V1[gop_index<br/>V1 字符串键]
        ROOT_V2[gop_v2<br/>V2 二进制键]
        EVENTS[events<br/>录制事件]
    end

    subgraph V2 层级结构
        ROOT_V2 --> CAM1_B[cam01]
        ROOT_V2 --> CAM2_B[cam02]
        CAM1_B --> D1[2026-03-15]
        CAM1_B --> D2[2026-03-16]
        D1 --> K1["[0x02][hash8][ts8] → GOPMeta JSON"]
        D1 --> K2["[0x02][hash8][ts8] → GOPMeta JSON"]
    end
```

**V2 二进制 Key（17 字节）：**

```
┌─────────┬──────────────────┬──────────────────┐
│ version │   camera_hash    │    timestamp     │
│  1 byte │    8 bytes       │    8 bytes       │
│  0x02   │  FNV-64a hash    │  UnixNano BE     │
└─────────┴──────────────────┴──────────────────┘
```

**性能对比：**

| 指标 | V1 字符串键 | V2 二进制键 |
|------|-----------|-----------|
| Key 大小 | ~30 bytes | 17 bytes |
| 比较速度 | 字符串比较 | 字节比较 |
| 相对性能 | 基线 | ~5x 更快 |

---

### 回放层 — Playback Server

> 文件：`internal/playback/server.go`、`fragment_reader.go`、`prefetcher.go`、`rtp_sender.go`

基于 gortsplib 实现的 RTSP Server，支持完整的点播回放信令。

```mermaid
sequenceDiagram
    participant C as RTSP Client
    participant S as Playback Server

    C->>S: DESCRIBE /playback/cam01?start=2026-03-16T08:00:00Z
    S-->>C: 200 OK + SDP (编码参数)

    C->>S: SETUP Transport: RTP/AVP/TCP
    S-->>C: 200 OK

    C->>S: PLAY Range: npt=0- Scale: 1.0
    S-->>C: 200 OK + RTP 数据流...

    Note over S: 支持中途 Seek 和变速
    C->>S: PLAY Range: npt=3600.0-
    S-->>C: 200 OK (跳转到 1 小时处)

    C->>S: PLAY Scale: 4.0
    S-->>C: 200 OK (切换 4 倍速, 丢弃 B 帧)

    C->>S: PAUSE
    S-->>C: 200 OK

    C->>S: TEARDOWN
    S-->>C: 200 OK
```

**URL 格式：**
```
rtsp://{host}:8554/playback/{camera_id}?start={ISO8601}&end={ISO8601}
```

**Seek 支持：**
- NPT 相对时间：`Range: npt=3600.0-`（从开始时间偏移 3600 秒）
- Clock 绝对时间：`Range: clock=20260316T083000Z-`

**预读取机制（Prefetcher）：**
- 当前 GOP 播放时异步预载下一个 GOP
- LRU 缓存最多 10 个 GOP
- Get 后自动从缓存移除（一次性消费）

---

### 内存控制器 — MemoryController

> 文件：`internal/memory/controller.go`

全局内存管理器，通过原子操作跟踪内存使用，实施四级背压策略。

```mermaid
graph LR
    subgraph 压力等级
        L0[Level 0<br/>< 60%<br/>正常录制]
        L1[Level 1<br/>60-80%<br/>加速 Flush]
        L2[Level 2<br/>80-90%<br/>丢弃 B 帧]
        L3[Level 3<br/>> 90%<br/>仅 I 帧]
    end

    L0 -->|使用率上升| L1
    L1 -->|使用率上升| L2
    L2 -->|使用率上升| L3
    L3 -->|使用率下降| L2
    L2 -->|使用率下降| L1
    L1 -->|使用率下降| L0
```

**集成点：**

| 组件 | 集成方式 |
|------|---------|
| StreamRecorder | `ProcessSample()` 中根据压力等级丢弃 B/P 帧 |
| AsyncWriter | `Submit()` 时 Allocate 内存配额，处理完 Release |
| REST API | `/system/stats` 返回 `used_bytes`、`max_bytes`、`pressure_level` |
| Prometheus | `memory_usage_bytes` + `memory_pressure_level` 指标 |

---

### 录制调度器 — ScheduleController

> 文件：`internal/recorder/scheduler.go`

支持按星期 + 时间段的计划任务录制，每 30 秒评估一次。

```yaml
# 配置示例：工作日 8:00-20:00 自动录制
cameras:
  - id: "cam01"
    recording:
      schedule:
        - weekdays: [1,2,3,4,5]  # 周一到周五
          start: "08:00"
          end: "20:00"
```

**跨午夜支持：** 配置 `start: "22:00"` / `end: "06:00"` 时，会检查前一天的星期是否匹配。

---

### 录像过期清理 — RetentionCleaner

> 文件：`internal/recorder/retention.go`

定时扫描并删除超过保留期的录像数据。

```mermaid
graph TB
    TIMER[每小时触发] --> SCAN[逐摄像头扫描]
    SCAN --> DAY[逐天遍历<br/>cutoff-30d → cutoff]
    DAY --> QUERY[QueryRange 查询当天 GOPs]
    QUERY --> DEL_STORE[Storage.Delete 删除对象]
    DEL_STORE --> DEL_IDX[DeleteRange 清理索引]
```

**清理策略：**
- 默认保留 30 天（可配置 `recorder.retention_days`）
- 每小时运行一次，启动后 30 秒首次执行
- 同时删除数据对象和 init segment（best effort）
- 使用 `BoltIndex.DeleteRange()` 批量清理索引

---

### 索引快照 — SnapshotManager

> 文件：`internal/index/snapshot.go`

定期创建 BoltDB 一致性快照并上传到 SeaweedFS，支持灾难恢复。

**工作流程：**

```mermaid
graph LR
    TICK[定时触发<br/>默认 300s] --> VIEW[BoltDB View 事务<br/>一致性读]
    VIEW --> BUF[WriteTo 内存 Buffer]
    BUF --> UPLOAD[Storage.Put 上传<br/>key: backups/index_snapshot.db]
```

**恢复流程：**

```go
// 1. 下载快照
index.RestoreFromSnapshot(storage, "/data/recordings.db")
// 2. 正常打开恢复后的数据库
idx, _ := index.NewBoltIndex("/data/recordings.db")
```

---

### REST API

> 文件：`internal/api/server.go`

| 方法 | 路径 | 说明 |
|------|------|------|
| `GET` | `/api/v1/recordings/{camera_id}?start=RFC3339&end=RFC3339` | 查询录像时间段 |
| `GET` | `/api/v1/recordings/{camera_id}/status` | 摄像头录制状态 + 今日统计 |
| `POST` | `/api/v1/recordings/{camera_id}/start` | 手动开始录像 |
| `POST` | `/api/v1/recordings/{camera_id}/stop` | 手动停止录像 |
| `GET` | `/api/v1/system/stats` | 系统状态（活跃路数、队列、内存） |

**响应示例：**

```json
// GET /api/v1/system/stats
{
  "active_recorders": 42,
  "writer": {
    "submitted": 128456,
    "failed": 3,
    "dropped": 0,
    "queue_len": 12
  },
  "memory": {
    "used_bytes": 2147483648,
    "max_bytes": 4294967296,
    "pressure_level": 0
  }
}
```

```json
// GET /api/v1/recordings/cam01/status
{
  "camera_id": "cam01",
  "state": "recording",
  "today": {
    "segments": 5,
    "total_duration": "8h32m15s"
  }
}
```

---

### 监控指标 — Prometheus Metrics

> 文件：`internal/metrics/metrics.go`，暴露于 `:9090/metrics`

**录制指标：**

| 指标名 | 类型 | 说明 |
|--------|------|------|
| `recording_streams_active` | Gauge | 当前录制路数 |
| `gops_written_total` | Counter (camera_id) | 已写入 GOP 总数 |
| `writer_queue_depth` | Gauge | 写入队列深度 |
| `writer_dropped_total` | Counter | 丢弃的写入任务数 |
| `storage_write_latency_seconds` | Histogram | 存储写入延迟 |
| `index_query_latency_seconds` | Histogram | 索引查询延迟 |

**内存指标：**

| 指标名 | 类型 | 说明 |
|--------|------|------|
| `memory_usage_bytes` | Gauge | 当前内存使用 |
| `memory_pressure_level` | Gauge | 背压等级 (0-3) |

**SeaweedFS 存储层指标：**

| 指标名 | 类型 | 说明 |
|--------|------|------|
| `seaweed_assign_latency_seconds` | Histogram | Master.Assign 延迟 |
| `seaweed_create_entry_latency_seconds` | Histogram | Filer.CreateEntry 延迟 |
| `seaweed_volume_put_latency_seconds` | Histogram | Volume HTTP PUT 延迟 |
| `seaweed_volume_get_range_latency_seconds` | Histogram | Volume HTTP GET/Range 延迟 |
| `seaweed_compensation_queue_size` | Gauge | 补偿队列大小 |
| `seaweed_compensation_retry_total` | Counter | 补偿重试次数 |
| `seaweed_orphan_suspected_total` | Counter | 疑似孤儿对象数 |
| `seaweed_hybrid_fallback_total` | Counter | 降级到 filer_http 次数 |

**回放指标：**

| 指标名 | 类型 | 说明 |
|--------|------|------|
| `playback_sessions_active` | Gauge | 活跃回放会话数 |

---

## 录制控制状态机

```mermaid
stateDiagram-v2
    [*] --> Idle

    Idle --> WaitingKeyFrame: Start() / 手动 / 计划任务
    WaitingKeyFrame --> Recording: 收到首个 I 帧
    Recording --> Stopping: Stop()
    Stopping --> Idle: flush 完成

    Recording --> WaitingKeyFrame: RTSP 断流后重连<br/>(enabled=true)
    WaitingKeyFrame --> Idle: Stop()

    note right of Recording
        编码切换时:
        flush 当前 GOP
        → 生成新 init segment
        → 继续录制 (不改变状态)
    end note

    note right of WaitingKeyFrame
        RTSP 连接与录制是
        两个独立的生命周期
        断线不改变录制意愿
    end note
```

**关键原则：**
- RTSP 连接始终保持（除非摄像头被移除）
- 录制由用户 / 计划任务 / 事件触发控制
- `enabled` 状态不因断线改变，重连后自动恢复到 `WaitingKeyFrame`
- 停止录制时：flush 当前 GOP + 写入 stop 事件到索引

---

## 存储对象结构

```
SeaweedFS 对象布局:

/recordings/{camera_id}/
  ├── init/{sha1_hash}.mp4init           # fMP4 init segment (~500B)
  │                                       # 编码参数不变时只存一份
  └── data/{YYYY-MM-DD}/
      ├── {unix_ms}-000000.frag          # GOP fragment #0 (0.5-2MB)
      ├── {unix_ms}-000001.frag          # GOP fragment #1
      └── ...
```

**对象数量估算：**
- GOP 间隔 2 秒 → 每路 1,800 个/小时 → 每路 43,200 个/天
- 500 路 × 30 天 = ~6.5 亿个对象
- SeaweedFS 设计目标即为百亿级小文件，完全可承载

---

## 索引 Key 设计

```mermaid
graph TB
    subgraph V1 字符串键 Legacy
        V1K["cam01:1710576000000000000"]
    end

    subgraph V2 二进制键 17 bytes
        V2K["0x02 | FNV64a(cam01) | BigEndian(unixNano)"]
        V2D["存储在 gop_v2/cam01/2026-03-16/ bucket 下"]
    end

    V1K -.->|迁移| V2K
```

**迁移工具：** `cmd/migrate/main.go`

```bash
# 干跑模式，查看将迁移的条目数
go run cmd/migrate/main.go -db ./data/recordings.db -dry-run

# 执行迁移
go run cmd/migrate/main.go -db ./data/recordings.db
```

---

## 内存背压策略

```mermaid
graph TB
    subgraph 内存使用率
        MEM[当前内存] --> CHECK{使用率?}
    end

    CHECK -->|< 60%| L0[Level 0: 正常<br/>全帧录制]
    CHECK -->|60-80%| L1[Level 1: 加速 Flush<br/>减小 GOP buffer]
    CHECK -->|80-90%| L2[Level 2: 丢弃 B 帧<br/>只录 I + P]
    CHECK -->|> 90%| L3[Level 3: 仅 I 帧<br/>其余全丢]

    L2 -->|丢弃| DROP_B[B 帧被 StreamRecorder 丢弃]
    L3 -->|丢弃| DROP_BP[B + P 帧被 StreamRecorder 丢弃]

    subgraph AsyncWriter 端
        ALLOC{Allocate 成功?}
        ALLOC -->|否| DROP_JOB[丢弃整个 WriteJob]
        ALLOC -->|是| ENQUEUE[入队]
        PROCESS[Worker 处理完成] --> RELEASE[Release 释放配额]
    end
```

**内存预算（1000 路）：**

| 组件 | 估算 |
|------|------|
| 每路 GOP buffer | ~2MB × 1000 = 2GB |
| 每路连接开销 | ~4MB × 1000 = 4GB |
| BoltDB + 连接池 | ~600MB |
| **总计** | **~6.6GB** |

---

## 倍速播放与抽帧策略

```mermaid
graph TB
    SCALE[播放倍速] --> CHECK{Scale 值?}

    CHECK -->|≤ 2x| FULL[全帧发送<br/>缩短帧间隔]
    CHECK -->|4x| DROP_B[丢弃 B 帧<br/>发送 I + P]
    CHECK -->|≥ 8x| I_ONLY[仅发送 I 帧]

    FULL --> TIMING[帧间隔 = 原始间隔 / Scale]
    DROP_B --> TIMING
    I_ONLY --> TIMING
```

**支持的倍速：** 1/8x, 1/4x, 1/2x, 1x, 2x, 4x, 8x, 16x

**RTP 时间戳策略：** 保持原始时间戳不变，通过控制发送节奏实现倍速。

---

## SeaweedFS 写入事务与补偿

```mermaid
stateDiagram-v2
    [*] --> Init: Put() 调用

    Init --> Assigned: Master.Assign 成功
    Init --> FallbackPut: Assign 失败 (3次重试后)

    Assigned --> DataUploaded: Volume PUT 成功
    Assigned --> Failed: PUT 失败 (3次重试后)

    DataUploaded --> Committed: CreateEntry 成功
    DataUploaded --> Compensating: CreateEntry 失败

    Compensating --> Committed: 补偿重试成功
    Compensating --> OrphanSuspected: 补偿 3 次仍失败

    FallbackPut --> Committed: Filer HTTP 降级写入成功
```

**补偿队列：**
- CreateEntry 失败 → 进入 `compQueue` (容量 1024)
- 后台协程消费，重试 3 次（50ms → 100ms → 200ms 退避）
- 超过重试阈值 → 标记为 `orphan_suspected`，上报 Prometheus 指标
- 按 object_key 去重，避免重复目录项

---

## 降级与容错

```mermaid
graph TB
    subgraph 正常模式 Hybrid
        HYBRID[gRPC 控制面<br/>+ HTTP 数据面]
    end

    subgraph 降级检测
        FAIL[控制面连续失败<br/>30s 内 ≥ 20 次] --> DEGRADE[切换到 filer_http]
    end

    subgraph 降级模式 FilerHTTP
        FHTTP[Filer HTTP PUT/GET<br/>一跳写入]
    end

    subgraph 恢复探测
        PROBE[每 5s Ping<br/>Master + Filer] --> RECOVER[恢复 Hybrid 模式]
    end

    HYBRID --> FAIL
    DEGRADE --> FHTTP
    FHTTP --> PROBE
    RECOVER --> HYBRID
```

**错误码映射：**

| 来源 | 状态 | 动作 | 最大重试 |
|------|------|------|---------|
| gRPC | UNAVAILABLE | 指数退避重试 | 3 |
| gRPC | DEADLINE_EXCEEDED | 重试 | 3 |
| HTTP | 500/502/503/504 | 指数退避重试 | 3 |
| HTTP | 404 (Volume) | 刷新 volumeCache 重试 | 1 |
| HTTP | 400/401/403 | 立即失败 | 0 |

---

## 项目结构

```
recording-server/
├── cmd/
│   ├── server/main.go                    # 启动入口，组装所有组件
│   └── migrate/main.go                   # 索引 V1→V2 迁移工具
├── internal/
│   ├── config/config.go                  # YAML 配置加载与校验
│   ├── puller/
│   │   └── rtsp_puller.go               # RTSP 拉流 (gortsplib Client)
│   ├── recorder/
│   │   ├── stream_recorder.go           # 单路录制状态机
│   │   ├── gop_assembler.go             # GOP 组装器
│   │   ├── fmp4_builder.go              # fMP4 封装 (mp4ff)
│   │   ├── async_writer.go              # 异步写入队列 + Worker 池
│   │   ├── scheduler.go                 # 计划任务录制调度
│   │   ├── retention.go                 # 录像过期删除
│   │   └── types.go                     # Codec/Sample/State 类型定义
│   ├── storage/
│   │   ├── interface.go                 # StorageBackend 接口 + DTO
│   │   ├── seaweed_hybrid.go            # 混合模式 (gRPC+HTTP)
│   │   ├── seaweed_filer_http.go        # Filer HTTP 降级实现
│   │   ├── grpc_master_client.go        # Master gRPC 客户端
│   │   ├── grpc_filer_client.go         # Filer gRPC 客户端
│   │   └── http_volume_client.go        # Volume HTTP 客户端
│   ├── index/
│   │   ├── interface.go                 # IndexStore 接口
│   │   ├── bolt_index.go                # BoltDB 实现 (V1+V2 双写)
│   │   └── snapshot.go                  # 索引快照备份与恢复
│   ├── playback/
│   │   ├── server.go                    # RTSP 点播服务
│   │   ├── fragment_reader.go           # fMP4 fragment 加载与解析
│   │   ├── prefetcher.go               # GOP 预读取 (LRU 10)
│   │   └── rtp_sender.go               # RTP 打包与发送
│   ├── memory/controller.go             # 全局内存控制器 (四级背压)
│   ├── metrics/metrics.go               # Prometheus 指标定义
│   └── api/server.go                    # REST API 服务
├── pb/                                   # SeaweedFS gRPC 生成代码
│   ├── master_pb/
│   ├── filer_pb/
│   ├── volume_server_pb/
│   └── remote_pb/
├── config.yaml                           # 默认配置文件
├── go.mod
└── go.sum
```

---

## 技术选型

| 模块 | 选型 | 说明 |
|------|------|------|
| RTSP 收流/发流 | [gortsplib](https://github.com/bluenviron/gortsplib) v4 | MediaMTX 底层库，Go 生态最成熟 |
| fMP4 封装/解析 | [mp4ff](https://github.com/Eyevinn/mp4ff) v0.46 | 支持 fragment 级别读写 |
| RTP 编解码 | [pion/rtp](https://github.com/pion/rtp) | Go 标准 RTP/RTCP 实现 |
| H.264/H.265 解析 | [mediacommon](https://github.com/bluenviron/mediacommon) | NALU/SPS/PPS/VPS 解析 |
| 分布式存储 | [SeaweedFS](https://github.com/seaweedfs/seaweedfs) | 擅长海量小文件，S3 兼容 |
| gRPC | [google.golang.org/grpc](https://google.golang.org/grpc) v1.65 | Master/Filer 控制面 |
| 录像索引 | [bbolt](https://github.com/etcd-io/bbolt) v1.3 | 嵌入式 B+ 树，纯 Go |
| 配置 | [yaml.v3](https://gopkg.in/yaml.v3) | YAML 配置解析 |
| 监控 | [prometheus/client_golang](https://github.com/prometheus/client_golang) | 指标暴露 |

---

## 配置说明

```yaml
server:
  rtsp_playback_port: 8554       # RTSP 回放端口
  api_port: 8080                 # REST API 端口
  metrics_port: 9090             # Prometheus 指标端口

memory:
  max_memory_mb: 4096            # 内存上限 (设为可用 RAM 的 80%)
  gop_buffer_pool_size: 2097152  # GOP buffer pool 大小 (2MB)

storage:
  seaweedfs:
    mode: "hybrid"               # hybrid (生产) / filer_http (简单部署)
    filer_http_url: "http://localhost:8888"
    master_grpc_addrs:           # hybrid 模式必填
      - "seaweedfs-master:19333"
    filer_grpc_addrs:            # hybrid 模式必填
      - "seaweedfs-filer:18888"
    volume_http_scheme: "http"
    grpc:
      keepalive_seconds: 30
      max_recv_msg_mb: 4
      max_send_msg_mb: 4
    fid_pool:
      batch_assign_count: 100    # 批量预分配 fid 数
      low_watermark: 20          # 低水位触发补充
    cache:
      volume_ttl_seconds: 300    # volume 路由缓存 TTL
      entry_lru_size: 10000      # entry 缓存 LRU 容量
      entry_ttl_seconds: 120     # entry 缓存 TTL

index:
  bolt_path: "/data/recordings.db"
  batch_interval_ms: 100         # 批量写入间隔
  snapshot_interval: 300         # 索引快照间隔 (秒)

recorder:
  segment_mode: "gop"            # gop (默认) / chunk
  chunk_max_gops: 15             # chunk 模式下最大 GOP 数
  async_writer_workers: 8        # 写入 worker 数 (1 per ~60 cameras)
  async_writer_queue_size: 1000  # 写入队列容量
  retry_max: 3                   # 存储重试次数
  retention_days: 30             # 录像保留天数

cameras:
  - id: "cam01"
    rtsp_url: "rtsp://admin:pass@192.168.1.64:554/stream"
    transport: "tcp"             # tcp / udp / auto
    enabled: true
    recording:
      auto_start: true
      schedule:
        - weekdays: [1,2,3,4,5]
          start: "08:00"
          end: "20:00"
```

---

## 快速开始

### 前置条件

- Go 1.23+
- 运行中的 SeaweedFS 集群（或单机 all-in-one）

### 构建与运行

```bash
# 构建
go build -o recording-server ./cmd/server/

# 验证配置
./recording-server --validate config.yaml

# 启动服务
./recording-server config.yaml
```

### 验证服务

```bash
# 查看系统状态
curl http://localhost:8080/api/v1/system/stats

# 查看 Prometheus 指标
curl http://localhost:9090/metrics

# 手动启停录制
curl -X POST http://localhost:8080/api/v1/recordings/cam01/start
curl -X POST http://localhost:8080/api/v1/recordings/cam01/stop

# 查看录像状态
curl http://localhost:8080/api/v1/recordings/cam01/status

# 查询录像时间段
curl "http://localhost:8080/api/v1/recordings/cam01?start=2026-03-16T00:00:00Z&end=2026-03-16T23:59:59Z"

# RTSP 回放 (使用 ffplay/VLC)
ffplay "rtsp://localhost:8554/playback/cam01?start=2026-03-16T08:00:00Z&end=2026-03-16T20:00:00Z"
```

---

## 开发与测试

```bash
# 安装依赖
go mod download

# 运行全部测试
go test ./...

# 运行特定包测试
go test ./internal/recorder/...
go test ./internal/index/...
go test ./internal/storage/...

# 运行单个测试
go test -run TestGOPAssembler ./internal/recorder/

# 基准测试
go test -bench=. -benchmem ./internal/recorder/
go test -bench=BenchmarkKeyEncoding ./internal/index/

# 代码检查
go vet ./...

# 索引迁移 (V1 → V2)
go run cmd/migrate/main.go -db ./data/recordings.db
```

---

## 性能目标

| 指标 | 目标值 | 验证方式 |
|------|--------|---------|
| 单节点录制路数 | 500-1000 路 1080P@25fps | 压测 |
| 每路录制内存 | ≤ 6MB | `memory_pressure_level` 指标 |
| 写入延迟 (P95) | < 3ms | `seaweed_volume_put_latency_seconds` |
| 索引查询 | < 1ms | `index_query_latency_seconds` |
| Seek 响应 | < 200ms | 端到端回放测试 |
| 首帧时间 | < 200ms | 端到端回放测试 |
| 崩溃数据丢失 | ≤ 1 个 GOP (~2 秒) | 仅最后一个未完成 fragment |
| 磁盘 IO (录制侧) | 0 | 全内存到网络 |

---

## 相关文档

- [`requirements-analysis.md`](requirements-analysis.md) — 完整系统需求与设计决策
- [`docs/plans/implementation-design.md`](docs/plans/implementation-design.md) — 详细实现规格
- [`docs/plans/2026-03-03-rtsp-recording-playback.md`](docs/plans/2026-03-03-rtsp-recording-playback.md) — 原始设计文档
