# 高性能 RTSP 录像存储与回放系统 — 需求分析文档

> **项目代号**：RecordingServer  
> **技术栈**：Go  
> **文档版本**：v1.1  
> **日期**：2026-03-03

---

## 一、项目背景与目标

### 1.1 项目背景

在专业安防监控场景中，需要对大量 IPC 摄像头的 RTSP 视频流进行 7×24 小时不间断录像，并支持事后回放查看。传统方案多依赖 C/C++ 技术栈（如 ZLMediaKit、live555）或 Java 平台（如 WVP-GB28181-PRO），存在技术栈混杂、二次开发成本高、运维复杂等问题。

本项目旨在构建一套**纯 Go 技术栈**的录像存储与回放服务，不含 Web 前端、不含设备管理，专注于录像的高性能写入、分布式存储和低延迟回放。

### 1.2 项目目标

- 从 RTSP 摄像头拉流并持续录制，存储至分布式对象存储 SeaweedFS
- 提供基于 RTSP 协议的录像点播回放，支持 seek 跳转和倍速播放
- 全内存数据通路，消除本地临时文件中转，实现零磁盘 IO 录制
- 支持大规模并发（单节点 500-1000 路 1080P），内存可控不 OOM
- 支持多编码格式（H.264 / H.265 / MPEG-4）及运行时编码动态切换
- 支持录像的启停控制，包括手动、计划任务、事件触发等模式
- 存储层采用 SeaweedFS **gRPC 控制面 + HTTP 数据面**混合模式，降低写入与回放延迟

### 1.3 不包含的内容

- Web 前端 / 移动端 UI
- 设备发现与管理（ONVIF / GB28181 信令）
- 用户权限管理
- 实时预览/直播分发（可作为扩展，但非核心需求）
- 视频转码

---

## 二、核心功能需求

### 2.1 录像录制

#### 2.1.1 RTSP 收流

- 系统作为 RTSP Client，主动连接 IPC 摄像头拉取视频流
- 支持 TCP / UDP / Auto 三种 RTP 传输模式
- 断线自动重连，可配置重连间隔和最大重试次数
- 每路摄像头独立一个拉流协程，互不影响

#### 2.1.2 视频编码支持

- 支持 H.264（AVC）、H.265（HEVC）、MPEG-4 Part 2 三种编码格式
- 通过 RTSP DESCRIBE 获取 SDP 信息自动识别编码格式
- 正确解析各编码格式的参数集：H.264 的 SPS/PPS、H.265 的 VPS/SPS/PPS、MPEG-4 的 VOL Header

#### 2.1.3 运行时编码切换兼容

当用户在 IPC 管理页面修改编码格式时（如从 H.264 改为 H.265），系统需要正确处理以下两种情况：

- **IPC 断开 RTSP 连接（常见行为）**：系统自动重连，通过新的 DESCRIBE 获取更新后的 SDP 和编码参数，生成新的 init segment 继续录制
- **IPC 不断连接、流内直接切换编码（少数型号）**：系统通过 NALU Header 结构差异实时检测编码格式变化（H.264 为 1 字节 Header，H.265 为 2 字节 Header），检测到变化后执行：flush 当前 GOP → 生成新 init segment → 等待新编码的首个 I 帧 → 恢复录制，同时在索引中写入编码切换事件

#### 2.1.4 录像格式

采用 **Fragmented MP4（fMP4）** 格式，每个 GOP 封装为一个独立的 fMP4 fragment：

- init segment（ftyp + moov）仅包含编码参数，编码参数不变时只存一份
- media fragment（moof + mdat）包含一个完整 GOP 的所有帧数据
- 每个 fragment 自包含索引，崩溃时仅丢失最后一个未完成的 fragment（约 2 秒数据）

#### 2.1.5 存储写入

- **零磁盘 IO 设计**：帧数据在内存中完成 GOP 组装和 fMP4 封装，直接通过网络写入 SeaweedFS
- **GOP 粒度即时写入**：每完成一个 GOP（约 2 秒）立即提交异步写入，每路录制内存占用控制在 6MB 以内
- **默认写入链路（混合模式）**：`Master gRPC Assign` 分配 fid → `Volume HTTP PUT` 直写数据 → `Filer gRPC CreateEntry` 注册元数据
- **兼容回退链路**：当 gRPC 控制面不可用时，自动降级到 `Filer HTTP PUT` 一跳写入路径
- **异步写入队列**：收帧与存储 IO 解耦，队列满时丢弃最旧数据防止 OOM
- **写入重试与补偿**：失败最多重试 3 次（指数退避）；若 Volume PUT 成功但 CreateEntry 失败，进入补偿队列重试注册，避免孤儿数据

#### 2.1.6 录像索引

采用 BoltDB 嵌入式 KV 数据库存储录像索引：

- 索引 Key 为 `{camera_id}:{unix_nano}`，B+ 树天然按时间有序
- 索引 Value 为 GOPMeta 紧凑二进制编码，包含：起止时间、SeaweedFS 对象 Key、init segment Key、数据大小、帧数量
- 支持 Batch 合并写入提升吞吐
- 索引查询性能 < 1ms

#### 2.1.7 录像过期删除

- 支持 SeaweedFS TTL 自动过期删除
- 支持定时扫描按日期目录批量删除
- 可配置保留天数，默认 30 天

### 2.2 录像回放

#### 2.2.1 RTSP 点播服务

系统作为 RTSP Server，接受客户端的录像回放请求：

- URL 格式：`rtsp://{host}:{port}/playback/{camera_id}?start={ISO8601}&end={ISO8601}`
- 支持完整的 RTSP 信令交互：DESCRIBE → SETUP → PLAY → PAUSE → TEARDOWN
- 支持 TCP 和 UDP 两种 RTP 传输模式

#### 2.2.2 Seek 跳转

- 通过 PLAY 请求的 `Range` 头指定跳转目标时间
- 支持 NPT 相对时间（`Range: npt=3600.0-`）和 Clock 绝对时间（`Range: clock=20240101T083000Z-`）
- 跳转流程：BoltDB 查询目标 GOP（<1ms）→ Filer gRPC 查询 entry（可缓存）→ Volume HTTP Range 读取 GOP 数据（目标 <20ms）→ 解析并发送
- 总体 seek 响应延迟目标 < 200ms（含客户端解码显示）

#### 2.2.3 倍速播放

- 通过 PLAY 请求的 `Scale` 头控制播放速度
- 支持的倍速范围：1/8x, 1/4x, 1/2x, 1x, 2x, 4x, 8x, 16x
- 倍速策略：低倍速（≤2x）全帧加速发送；中倍速（4x）丢弃 B 帧只发 I+P；高倍速（≥8x）只发 I 帧
- 倍速切换不需要 seek，只改变发送行为
- 帧发送使用绝对时间点计算避免累积误差

#### 2.2.4 分段无缝衔接

- 录像按 GOP 独立存储，回放时需要无缝串联连续 GOP
- 预读取机制：当前 GOP 播放进度达到 70% 时，异步预载下一个 GOP
- 高倍速场景下提升预读取深度

#### 2.2.5 跨编码回放

- 当回放跨越编码切换点时（如从 H.264 段切换到 H.265 段），系统自动检测 init_key 变化
- 加载新编码的 init segment，先发送新的参数集（SPS/PPS/VPS），再发送 I 帧
- 更新 RTP 打包器适配新编码格式

### 2.3 录制控制

#### 2.3.1 录制启停

- RTSP 连接与录制是两个独立的生命周期，RTSP 连接始终保持，录制按需开关
- 录制状态机：Idle → WaitingKeyFrame → Recording → Stopping → Idle
- 停止录制时：flush 当前 GOP + 写入 stop 事件到索引
- 开始录制时：生成新 recordingID + 等待第一个 I 帧
- 断线恢复：录制意愿（enabled）不因断线改变，重连成功后自动恢复到 WaitingKeyFrame 状态

#### 2.3.2 计划任务录制

- 支持按星期 + 时间段配置录制计划
- 定时器每 30 秒检查一次当前是否在录制时间段内
- 自动开始/停止录制

#### 2.3.3 录像时间段管理

- 索引中记录完整的录制事件链（start / stop / disconnect / reconnect / codec_change）
- 可从事件链推导出精确的录像连续时间段
- REST API 返回指定日期的录像时间段列表，用于客户端时间轴展示

---

## 三、非功能需求

### 3.1 性能指标

| 指标 | 目标值 |
|------|-------|
| 单节点录制路数 | 500-1000 路 1080P@25fps |
| 单路码率范围 | 2-8 Mbps（典型 4Mbps） |
| 每路录制内存 | ≤ 6MB（GOP 粒度写入模式） |
| 全局录制内存（1000 路） | ≤ 6.6GB（含 BoltDB、连接池等全局开销） |
| 混合模式单次写入延迟 | < 3ms（gRPC Assign + HTTP PUT + gRPC CreateEntry） |
| 混合模式单次读取延迟 | < 20ms（元数据缓存命中后仅 HTTP Range） |
| Seek 响应延迟 | < 200ms（请求到出画面） |
| 索引查询延迟 | < 1ms |
| SeaweedFS 单次 GET/Range 延迟 | < 20ms（直读 Volume 目标值） |
| 首帧时间 | < 200ms |
| 崩溃数据丢失 | ≤ 1 个 GOP（约 2 秒） |
| 磁盘 IO（录制服务器侧） | 0（全内存到网络） |

### 3.2 可靠性

- 录制服务崩溃后，最多丢失 1 个 GOP 的数据，已写入 SeaweedFS 的数据不受影响
- BoltDB 索引支持定期 snapshot 到 SeaweedFS，崩溃后可从 snapshot 恢复或从 SeaweedFS 目录遍历重建
- 异步写入队列满时降级丢帧，不会导致 OOM
- RTSP 断流后自动重连，录制意愿不变

### 3.3 内存管理

采用 MemoryController 全局内存管理器，实施四级背压策略：

| 压力等级 | 内存使用率 | 降级行为 |
|---------|-----------|---------|
| Level 0 | < 60% | 正常录制全部帧 |
| Level 1 | 60-80% | 加速 flush，减小 GOP buffer 阈值 |
| Level 2 | 80-90% | 丢弃 B 帧，只录制 I + P 帧 |
| Level 3 | > 90% | 只录制 I 帧，其余全丢 |

关键内存优化措施：

- sync.Pool 复用 GOP buffer，减少 GC 压力
- 异步写入队列释放内存配额后才算完成
- 预读取缓存限制：每个回放 session 最多缓存 1 个预读 GOP

### 3.4 可扩展性

- 录制节点可水平扩展，按摄像头数量分配（每节点 200-500 路）
- SeaweedFS 存储节点按容量水平扩展
- 回放节点可与录制节点合部署或独立部署
- 每个录制节点本地 BoltDB 索引，回放时路由到对应节点

### 3.5 可观测性

暴露 Prometheus 格式的监控指标，覆盖以下维度：

- 录制指标：写入 GOP 数、写入字节数、丢弃 GOP 数、写入延迟分布、写入队列长度
- 内存指标：当前使用量、上限、背压等级
- 回放指标：活跃会话数、seek 延迟分布、预读取命中率
- 存储指标：SeaweedFS PUT/GET 延迟
- 连接指标：各摄像头连接状态、重连次数

---

## 四、技术架构

### 4.1 系统架构总览

```
┌──────────────────────────────────────────────────────────┐
│                 Recording Server (Go)                     │
│                                                           │
│  收流层                                                    │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐               │
│  │ Puller   │  │ Puller   │  │ Puller   │  ... × N      │
│  │ cam01    │  │ cam02    │  │ cam03    │               │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘               │
│       │              │              │                     │
│  录制层                                                    │
│  ┌────▼─────┐  ┌─────▼────┐  ┌─────▼────┐               │
│  │ GOP      │  │ GOP      │  │ GOP      │               │
│  │ Assembler│  │ Assembler│  │ Assembler│               │
│  └────┬─────┘  └─────┬────┘  └─────┬────┘               │
│       │               │              │                    │
│  ┌────▼─────┐  ┌──────▼───┐  ┌──────▼───┐               │
│  │ fMP4     │  │ fMP4     │  │ fMP4     │               │
│  │ Builder  │  │ Builder  │  │ Builder  │               │
│  └────┬─────┘  └──────┬───┘  └──────┬───┘               │
│       └───────────────┼──────────────┘                    │
│                       ▼                                   │
│  异步写入层                                                │
│  ┌───────────────────────────────────┐                    │
│  │  WriteJob Channel (cap=1000)      │                    │
│  │  → N 个 Writer Goroutine          │                    │
│  └───────────┬───────────┬───────────┘                    │
│              │           │                                │
│      ┌───────▼────────┐  ┌───▼──────┐                     │
│      │SeaweedHybrid   │  │ BoltDB   │                     │
│      │gRPC控制+HTTP数据│ │ 索引写入  │                     │
│      └────────────────┘  └──────────┘                     │
│                                                           │
│  ──────────── 分隔线 ────────────                          │
│                                                           │
│  回放层                                                    │
│  ┌────────────────────────────────────┐                   │
│  │ RTSP Playback Server (gortsplib)   │                   │
│  │                                     │                   │
│  │ Session Manager                     │                   │
│  │   ├ Session 1 (cam01, 2x)          │                   │
│  │   ├ Session 2 (cam03, 1x)          │                   │
│  │   └ ...                             │                   │
│  │                                     │                   │
│  │ GOP Reader → fMP4 Parser            │                   │
│  │ → RTP Packer → gortsplib Send       │                   │
│  │                                     │                   │
│  │ Prefetcher: 70% 触发异步预载        │                   │
│  └────────────────────────────────────┘                   │
│                                                           │
│  全局控制                                                  │
│  ┌─────────────────────────────────────────┐              │
│  │ MemoryController │ Config │ Metrics     │              │
│  └─────────────────────────────────────────┘              │
└──────────────────────────────────────────────────────────┘
```

### 4.2 数据流

录制数据流（零磁盘 IO）：

```
Camera RTSP → RTP 解包 → H264/H265 NALUs
→ GOP 组装（内存）→ fMP4 fragment 封装（内存）
→ 异步写入队列（内存）
→ Master gRPC Assign（fid）
→ Volume HTTP PUT（直写数据）
→ Filer gRPC CreateEntry（注册元数据）
→ BoltDB 索引
```

回放数据流：

```
Client PLAY(Range+Scale)
→ BoltDB 索引查询（<1ms）→ Filer gRPC LookupDirectoryEntry（可缓存）
→ Master/Filer gRPC LookupVolume（可缓存）
→ Volume HTTP GET/Range（<20ms）
→ fMP4 fragment 解析 → H264/H265 NALUs
→ RTP 打包 → gortsplib 发送 → Client
```

### 4.3 存储对象结构

```
SeaweedFS 对象布局:

/recordings/{camera_id}/
  ├── init/{codec_hash}.mp4init          # fMP4 init segment (~500B)
  └── data/{YYYY-MM-DD}/
      ├── {unix_millisecond}.frag        # 每个 GOP 一个独立对象 (0.5-2MB)
      ├── {unix_millisecond}.frag
      └── ...
```

对象数量：每路 1,800 个/小时（GOP 间隔 2 秒），SeaweedFS 设计目标即为百亿级小文件，完全可承载。若需减少对象数量，可启用 Chunk 合并模式（15 个 GOP 合并为一个对象，对象数减少 15 倍）。

---

## 五、技术选型

### 5.1 核心依赖

| 模块 | 选型 | 说明 |
|------|------|------|
| RTSP 收流/发流 | `bluenviron/gortsplib` | MediaMTX 底层库，Go 生态最成熟的 RTSP 实现 |
| fMP4 封装/解析 | `Eyevinn/mp4ff` | 支持 fMP4 fragment 级别的读写操作 |
| RTP 编解码 | `pion/rtp` + `pion/rtcp` | Go 标准 RTP/RTCP 实现 |
| H264/H265 解析 | `bluenviron/mediacommon` | NALU 解析、SPS/PPS/VPS 提取 |
| 分布式存储 | `SeaweedFS` | S3 兼容 API，擅长海量小文件存储 |
| gRPC 客户端 | `google.golang.org/grpc` | Master/Filer 控制面调用（Assign、CreateEntry、Lookup） |
| Protobuf | `google.golang.org/protobuf` | SeaweedFS proto 代码生成与序列化 |
| 录像索引 | `go.etcd.io/bbolt` | 嵌入式 B+ 树 KV，纯 Go 无 CGO |
| 配置管理 | `gopkg.in/yaml.v3` | YAML 配置文件 |
| 日志 | `rs/zerolog` | 结构化高性能日志 |
| 监控 | `prometheus/client_golang` | Prometheus 指标暴露 |

### 5.2 选型对比说明

**索引引擎选择 BoltDB 而非 SQLite 的原因**：纯 Go 无 CGO 交叉编译简单；B+ 树天然按时间有序支持 range scan；录像索引的单写多读模式与 BoltDB 完美匹配；内存占用更低。如果写入吞吐成为瓶颈（超 1000 路），可切换为 BadgerDB。

**存储格式选择 fMP4 而非传统 MP4 的原因**：每个 fragment 自包含索引可流式写入；崩溃安全（只丢最后一个 fragment）；天然适合对象存储（每个 GOP 可独立存储）；无需本地文件系统做中转。

**SeaweedFS 访问方式选择“gRPC 控制面 + HTTP 数据面”混合模式**：Master/Filer 的元数据与控制操作通过 gRPC（二进制协议、连接复用、低开销），文件数据读写通过 Volume HTTP PUT/GET（支持 Range，且目前 SeaweedFS Volume gRPC 不提供通用文件数据读写方法）；相比纯 Filer HTTP 路径减少中转跳数，同时避免 FUSE 在随机 seek 下的性能损耗。

**SeaweedFS 磁盘选择 XFS 文件系统而非裸盘直写**：SeaweedFS 的写入模式为大文件（30GB .dat）顺序追加，文件系统在此模式下开销仅 3-5%；优化 XFS 参数（noatime、allocsize=1g）后差距缩小到 1-3%；裸盘直写会失去 Page Cache 写缓冲、预读加速、崩溃一致性、运维工具等关键能力，得不偿失。

---

## 六、RTSP 协议实现要点

### 6.1 倍速播放原理

RTSP PLAY 请求中通过 `Scale` 头字段控制播放速度，`Scale: 2.0` 表示 2 倍速，`Scale: -1.0` 表示倒放。

服务端实现策略分为两种：低倍速（≤2x）全帧发送但缩短帧间发送间隔（如 25fps 原始帧间隔 40ms，2 倍速时按 20ms 间隔发送）；高倍速（≥4x）采用抽帧发送策略（只发 I 帧或 I+P 帧）。

RTP 时间戳处理采用"保持原始时间戳不变"的策略，通过控制发送节奏实现倍速，客户端在倍速模式下按"收到即渲染"方式工作。

### 6.2 Seek 跳转原理

通过 PLAY 请求的 `Range` 头指定播放起始时间。服务端解析目标时间后，在录像索引中定位包含该时间点的 GOP，优先通过 gRPC 获取元数据并缓存路由，再从 Volume 执行 HTTP Range 读取对应 GOP 数据，从 I 帧开始发送 RTP 流。

关键约束：seek 必须从 I 帧（关键帧）开始，因为 P/B 帧依赖前序帧解码。GOP 间隔越大，seek 精度越低（通常 2 秒 GOP 间隔，seek 精度约 2 秒）。

### 6.3 RTP 打包

遵循 RFC 6184（H.264）和 RFC 7798（H.265）：NALU 小于 MTU（1400B）时使用 Single NALU Packet 直接打包；大于 MTU 时使用 FU-A 分片打包为多个 RTP 包。帧的最后一个 RTP 包设置 Marker bit。

在 seek 跳转和编码切换后，需要在视频帧之前先发送 SPS/PPS（H.264）或 VPS/SPS/PPS（H.265），确保客户端解码器能正确初始化。

---

## 七、录制控制状态机

```
                     ┌──────────┐
                     │   Idle   │ 初始 / 停止后
                     └────┬─────┘
                          │ StartRecording()
                          ▼
                ┌───────────────────┐
                │ WaitingKeyFrame   │ RTSP 已连接，等待首个 I 帧
                └────────┬──────────┘
                         │ 收到 I 帧
                         ▼
                ┌───────────────────┐
          ┌────>│   Recording       │ 正常录制中
          │     └──┬─────┬────┬────┘
          │        │     │    │ StopRecording()
          │        │     │    ▼
          │        │     │ ┌────────────┐
          │        │     │ │  Stopping  │ 等待当前 GOP flush
          │        │     │ └─────┬──────┘
          │        │     │       │ flush 完成
          │        │     │       ▼
          │        │     │    ┌──────┐
          │        │     │    │ Idle │
          │        │     │    └──────┘
          │        │     │
          │        │     │ RTSP 断流
          │        │     ▼
          │     ┌──────────────┐
          │     │ Disconnected │ 等待重连
          │     └──────┬───────┘
          │            │ 重连成功 (enabled=true)
          └────────────┘ → WaitingKeyFrame
```

关键设计原则：RTSP 连接与录制是两个独立的生命周期。RTSP 连接始终保持（除非摄像头被移除），录制由用户 / 计划任务 / 事件触发控制。断线不改变录制意愿（enabled 状态），重连后若 enabled=true 自动恢复录制。

---

## 八、存储与部署

### 8.1 存储容量规划

假设：1080P H.264, 码率 4Mbps, 25fps, GOP 2 秒。

| 路数 | 7 天 | 15 天 | 30 天 | 90 天 |
|------|------|-------|-------|-------|
| 100 | 30 TB | 65 TB | 130 TB | 389 TB |
| 500 | 151 TB | 324 TB | 648 TB | 1,944 TB |
| 1000 | 302 TB | 648 TB | 1,296 TB | 3,888 TB |

注：实际值通常更低（非 24 小时录像、VBR 码率波动等）。

### 8.2 SeaweedFS 部署配置

SeaweedFS 裸盘处理推荐方案：对裸盘执行 `mkfs.xfs` 格式化后挂载到专用目录，不影响系统盘任何路径。

XFS 挂载优化参数：`noatime,nodiratime,logbufs=8,logbsize=256k,allocsize=1g,inode64`

内核调优：dirty_ratio=40、dirty_background_ratio=10、dirty_expire_centisecs=6000；磁盘调度器 SSD 用 none、HDD 用 bfq；预读 blockdev --setra 4096。

SeaweedFS 自身参数：compactionMBps=50 限制后台整理速度、fileSizeLimitMB=10 单对象上限、minFreeSpacePercent=5 磁盘保护。

### 8.3 部署架构

**单机部署**（≤200 路）：录制服务 + SeaweedFS all-in-one 部署在一台 16 核 32GB 服务器上。

**集群部署**（>200 路）：多个录制节点按摄像头分组，每节点 200-500 路；SeaweedFS 独立集群部署（Master ×3、Volume ×N、Filer ×2）；负载均衡器按 camera_id 路由。

---

## 九、配置文件设计

```yaml
server:
  rtsp_playback_port: 8554
  api_port: 8080
  metrics_port: 9090

memory:
  max_memory_mb: 4096
  gop_buffer_pool_size: 2097152     # 2MB

storage:
  seaweedfs:
    mode: "hybrid"                       # hybrid / filer_http
    master_grpc_addrs: ["seaweedfs-master-1:19333","seaweedfs-master-2:19333","seaweedfs-master-3:19333"]
    filer_grpc_addrs: ["seaweedfs-filer-1:18888","seaweedfs-filer-2:18888"]
    volume_http_scheme: "http"
    filer_http_url: "http://seaweedfs-filer:8888"   # 回退路径
    grpc:
      keepalive_seconds: 30
      max_recv_msg_mb: 4
      max_send_msg_mb: 4
      master_pool_size: 2
      filer_pool_size: 4
    fid_pool:
      batch_assign_count: 100
      low_watermark: 20
    cache:
      volume_ttl_seconds: 300
      entry_lru_size: 10000
      entry_ttl_seconds: 120
    security:
      enable_mtls: false
      ca_file: ""
      cert_file: ""
      key_file: ""
      jwt_token: ""

index:
  bolt_path: "/data/index/recordings.db"
  batch_interval_ms: 100
  snapshot_interval: 300              # 索引备份间隔（秒）

recorder:
  segment_mode: "gop"                 # gop / chunk
  chunk_max_gops: 15
  retention_days: 30
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
      schedule:
        - weekdays: [1,2,3,4,5]
          start: "08:00"
          end: "20:00"

  - id: "cam02"
    rtsp_url: "rtsp://admin:pass@192.168.1.65:554/Streaming/Channels/101"
    transport: "auto"
    enabled: true
    recording:
      auto_start: true
```

---

## 十、项目结构

```
recording-server/
├── cmd/server/main.go                    # 启动入口
├── internal/
│   ├── config/config.go                  # 配置加载与验证
│   ├── puller/
│   │   ├── manager.go                    # Puller 管理器（启停/重连）
│   │   └── rtsp_puller.go               # 单路 RTSP 拉流
│   ├── recorder/
│   │   ├── manager.go                    # Recorder 管理器
│   │   ├── gop_assembler.go             # GOP 组装器
│   │   ├── codec_detector.go            # 编码格式检测（运行时切换）
│   │   ├── fmp4_builder.go              # fMP4 init/fragment 封装
│   │   ├── stream_recorder.go           # 单路录制器（状态机）
│   │   ├── async_writer.go              # 异步写入
│   │   └── chunk_writer.go              # 可选 Chunk 合并
│   ├── storage/
│   │   ├── interface.go                  # 存储接口
│   │   ├── seaweed_hybrid.go            # SeaweedFS 混合实现（gRPC+HTTP）
│   │   ├── seaweed_filer_http.go        # Filer HTTP 回退实现
│   │   ├── grpc_master_client.go        # Master gRPC（Assign/Lookup）
│   │   ├── grpc_filer_client.go         # Filer gRPC（Entry 管理）
│   │   ├── fid_pool.go                  # fid 预分配池
│   │   ├── volume_cache.go              # volume 路由缓存
│   │   └── entry_cache.go               # entry 元数据缓存
│   ├── index/
│   │   ├── interface.go                  # 索引接口
│   │   ├── bolt_index.go                # BoltDB 实现
│   │   ├── codec.go                     # GOPMeta 二进制编解码
│   │   └── models.go                    # 数据结构
│   ├── playback/
│   │   ├── rtsp_server.go               # RTSP 点播服务
│   │   ├── handler.go                   # RTSP 信令处理
│   │   ├── session.go                   # 回放会话状态管理
│   │   ├── fragment_reader.go           # fMP4 解析 → sample 迭代
│   │   ├── rtp_sender.go               # RTP 打包发送 + 定时
│   │   └── prefetcher.go               # GOP 预读取
│   ├── memory/controller.go             # 全局内存控制器
│   └── api/query.go                     # REST API（录像查询）
├── pkg/fmp4/                             # fMP4 工具包
│   ├── init_builder.go
│   ├── fragment_builder.go
│   └── fragment_parser.go
├── config.yaml
├── go.mod
├── Dockerfile
├── docker-compose.yaml
└── README.md
```

---

## 十一、接口定义

### 11.1 RTSP 回放接口

```
URL: rtsp://{host}:{port}/playback/{camera_id}?start={ISO8601}&end={ISO8601}

DESCRIBE → 200 OK (SDP)
SETUP (Transport: RTP/AVP/TCP) → 200 OK
PLAY (Range: npt=0-, Scale: 1.0) → 200 OK + RTP 数据流
PLAY (Range: npt=1832.0-) → Seek 到 30:32
PLAY (Scale: 4.0) → 切换 4 倍速
PAUSE → 暂停
TEARDOWN → 释放资源
```

### 11.2 REST API 接口

```
GET /api/v1/recordings/{camera_id}/ranges?date=2024-01-01
  → 返回该日期的录像时间段列表

GET /api/v1/recordings/{camera_id}/status
  → 返回该摄像头的录制状态（是否在录、连接状态、今日统计）

GET /api/v1/system/stats
  → 返回系统整体状态（路数、内存、活跃会话数、写入队列长度）

POST /api/v1/recordings/{camera_id}/start
  → 开始录像

POST /api/v1/recordings/{camera_id}/stop
  → 停止录像
```

---

## 十二、关键风险与应对

| 风险 | 影响 | 应对措施 |
|------|------|---------|
| SeaweedFS 写入慢/不可用 | 录像丢失 | 异步队列缓冲 + 重试 3 次 + 队列满时降级丢帧 + 告警 |
| 内存不足 OOM | 进程崩溃 | MemoryController 四级背压降级，提前丢帧保进程 |
| BoltDB 索引损坏 | 无法检索录像 | 定期 snapshot 到 SeaweedFS + 可从对象列表重建索引 |
| 摄像头断流 | 录像中断 | 自动重连（5 秒间隔）+ 断流时间记录到索引 |
| 回放并发过高 | 响应慢 | 限制最大并发 session 数 + SeaweedFS 读缓存 |
| GOP 对象数量过多 | SeaweedFS 元数据压力 | 可选 Chunk 合并模式减少 15 倍对象数 |
| 时钟不同步 | 索引时间偏移 | NTP 同步 + 优先用 RTP 时间戳做相对计算 |
| IPC 中途切换编码 | 录像断裂/花屏 | CodecDetector 实时检测 + 自动生成新 init segment |
| SeaweedFS proto 版本漂移 | gRPC 调用失败/字段不兼容 | 锁定 SeaweedFS 版本 + CI 校验 proto 变更 + 重新生成 pb 代码 |
| Volume PUT 成功但 CreateEntry 失败 | 产生孤儿数据 | 补偿队列重试 CreateEntry + 定时孤儿扫描清理 |
| Volume 地址漂移（迁移/故障） | 读写短暂失败 | volume 路由 TTL 缓存 + 失败后强制刷新 LookupVolume 再重试 |
| gRPC 连接抖动 | 控制面超时增加 | 多地址连接池 + keepalive + 指数退避重连 + 熔断降级到 Filer HTTP |

---

## 十三、开发路线图

```
阶段一：基础录制（2-3 周）
  ├ RTSP 收流（gortsplib Client）
  ├ GOP 组装器
  ├ fMP4 fragment 封装（Eyevinn/mp4ff）
  ├ 基线存储链路（Filer HTTP PUT）
  └ BoltDB 索引写入

阶段二：基础回放（2-3 周）
  ├ RTSP 点播服务（gortsplib Server）
  ├ DESCRIBE / SETUP / PLAY / PAUSE / TEARDOWN 处理
  ├ fMP4 fragment 解析 → H264 NALUs
  ├ RTP 打包发送
  └ 分段无缝衔接

阶段三：回放增强（1-2 周）
  ├ SeaweedFS 混合存储改造（gRPC 控制面 + HTTP 数据面）
  ├ fid 预分配池 + volume/entry 缓存
  ├ 元数据补偿机制（防孤儿）
  ├ Seek 跳转（Range 头解析 + 索引查询 + Range 读取）
  ├ 倍速播放（Scale 头解析 + 帧间隔调整 + 抽帧策略）
  └ GOP 预读取

阶段四：健壮性（1-2 周）
  ├ 内存背压控制
  ├ 录制启停状态机
  ├ 编码动态切换检测与处理
  ├ 断线重连与自动恢复
  └ 计划任务录制

阶段五：运维与监控（1 周）
  ├ Prometheus 指标暴露
  ├ REST API（录像查询、系统状态）
  ├ 配置热加载
  ├ 索引 snapshot 与恢复
  └ Docker Compose 编排

预计总工期：8-12 周
```

---

## 十四、讨论过程中的关键技术决策记录

| # | 议题 | 决策 | 理由 |
|---|------|------|------|
| 1 | 开源整合 vs 自研 | 自研，参考开源库 | 开源方案（ZLMediaKit/WVP）为 C++/Java 混合栈，无法满足纯 Go 需求；LdDl/video-server 可借鉴但需大幅改造 |
| 2 | 传统 MP4 vs fMP4 | fMP4 | 崩溃安全、流式写入、无需本地中转 |
| 3 | 5 分钟攒 segment vs GOP 即时写入 | GOP 即时写入 | 5 分钟攒段 1000 路需 150GB 内存不可行；GOP 即时写入 1000 路仅需 6.6GB |
| 4 | SQLite vs BoltDB | BoltDB | 纯 Go 无 CGO、B+ 树天然有序、查询 <1ms、内存占用低 |
| 5 | SeaweedFS FUSE 挂载 vs HTTP API | HTTP API | FUSE 随机 seek 性能差；HTTP Range 精确读取所需部分 |
| 6 | SeaweedFS 裸盘直写 vs XFS 文件系统 | XFS | SeaweedFS 为大文件顺序追加写入，文件系统开销仅 3-5%；裸盘失去 Page Cache、预读、崩溃恢复、运维工具 |
| 7 | 每 GOP 一个对象 vs Chunk 合并 | 默认 GOP，可选 Chunk | GOP 模式内存最低（6MB/路）、seek 最简单；对象多但 SeaweedFS 可承载；Chunk 作为可选配置减少对象数 |
| 8 | 直写 Filer vs 直写 Volume Server | 默认混合模式（Master/Filer gRPC + Volume HTTP） | 数据面直写 Volume 降低中转延迟；控制面 gRPC 提升元数据操作效率并可预分配 fid |
| 9 | SeaweedFS 协议统一 | 不采用“全 gRPC” | 当前 Volume gRPC 不提供通用文件数据读写接口，数据面必须保留 HTTP PUT/GET |

---

## 十五、SeaweedFS gRPC 接口能力专题（v1.1 新增）

### 15.1 接口能力现状

SeaweedFS 对外暴露了 Master、Volume、Filer 三类 gRPC 接口，proto 定义位于：

- `weed/pb/master.proto`（Master）
- `weed/pb/volume_server.proto`（Volume）
- `weed/pb/filer.proto`（Filer）

默认端口规则为“HTTP 端口 + 10000”：

- Master：`9333`（HTTP）→ `19333`（gRPC）
- Volume：`8080`（HTTP）→ `18080`（gRPC）
- Filer：`8888`（HTTP）→ `18888`（gRPC）

各组件与录像系统相关能力：

- Master gRPC：`Assign`、`Lookup`、`Statistics`、`VolumeList` 等，适合写入前分配 fid 和路由查询
- Volume gRPC：偏管理面（状态、下线、批删、vacuum），不提供通用文件内容 PUT/GET
- Filer gRPC：`LookupDirectoryEntry`、`CreateEntry`、`DeleteEntry`、`ListEntries`、`SubscribeMetadata` 等，适合元数据读写与订阅

### 15.2 关键架构结论

SeaweedFS 的控制面和数据面是分离的，结论如下：

- 可行：`gRPC（控制面）+ HTTP（数据面）`
- 不可行：`全 gRPC 替代 HTTP`

原因：Volume Server 当前 gRPC 主要用于管理操作，录像数据的实际上传/读取仍依赖 HTTP PUT/GET（含 Range）。

### 15.3 混合模式链路设计

写入链路（录制）：

1. `Master.Assign`（gRPC）获取 fid 和 volume 路由
2. `Volume HTTP PUT` 直写数据
3. `Filer.CreateEntry`（gRPC）注册路径元数据

读取链路（回放）：

1. `Filer.LookupDirectoryEntry`（gRPC）查 entry/chunks（可缓存）
2. `LookupVolume`（gRPC）查 volume 地址（可缓存）
3. `Volume HTTP GET/Range` 读取数据

删除链路（过期清理）：

1. `Filer.ListEntries`（gRPC）枚举对象
2. `Filer.DeleteEntry`（gRPC）删除目录项
3. `Volume.BatchDelete`（gRPC）批量回收 fid（按运维策略启用）

### 15.4 对录像系统模块的影响

录制模块：

- `SeaweedStorage.Put` 从单一 HTTP 调整为三段式混合调用
- 新增 fid 预分配池，降低 Assign 请求频率
- 新增补偿队列处理“PUT 成功但 CreateEntry 失败”的孤儿风险

回放模块：

- `SeaweedStorage.GetRange` 首次读取先做 gRPC 元数据查询
- 命中 `entryCache` 与 `volumeCache` 后，后续仅走 HTTP Range
- 连续播放同一 GOP 时可达到高缓存命中，降低 seek 抖动

索引备份与治理：

- snapshot 上传可继续走 HTTP PUT（实现简单）
- 元数据订阅建议使用 `SubscribeMetadata`（gRPC）做实时可用性感知

### 15.5 连接与缓存规范

建议默认参数：

- Master gRPC 连接池：`1-2` 个长连接
- Filer gRPC 连接池：`2-4` 个长连接
- Keepalive：`30s`
- 消息大小：`4MB`
- fid 预分配：每批 `100`，低水位 `20` 触发补充
- `volumeCache`：TTL `300s`
- `entryCache`：LRU `10000` + TTL `120s`

### 15.6 性能基线（目标值）

| 指标 | 纯 HTTP（基线） | 混合模式（目标） |
|------|------------------|------------------|
| 录制单次写入延迟 | ~5ms | ~3ms |
| 回放单次读取延迟 | ~40ms | ~20ms |
| 批量删除（10 万对象） | ~300s | ~30s |

说明：以上为需求目标与容量规划基线，最终以压测结果为准并在 `docs/quality` 中固化。

### 15.7 存储层抽象约束

上层模块只依赖统一接口，不感知协议细节：

```go
type StorageBackend interface {
    Put(key string, data []byte) error
    Get(key string) ([]byte, error)
    GetRange(key string, offset, length int64) ([]byte, error)
    Delete(key string) error
    List(prefix string) ([]string, error)
}
```

推荐实现：

- `SeaweedHybridStorage`：默认实现（gRPC 控制面 + HTTP 数据面）
- `SeaweedFilerHTTPStorage`：降级与兼容实现

### 15.8 接口设计草案（字段级）

以下为存储层最小可实施接口，按“内部抽象字段”定义，屏蔽 SeaweedFS proto 细节漂移。

#### 15.8.1 写入控制面：Master.Assign

| 项 | 字段 | 类型 | 必填 | 说明 |
|----|------|------|------|------|
| 请求 | count | uint32 | 否 | 一次申请 fid 数量，默认 1，预分配场景设为 100 |
| 请求 | collection | string | 否 | 对象集合，默认 `recordings` |
| 请求 | replication | string | 否 | 副本策略（如 `001`） |
| 请求 | ttl_sec | int64 | 否 | 对象 TTL（秒） |
| 请求 | data_center | string | 否 | 就近路由 |
| 请求 | rack | string | 否 | 机架亲和 |
| 响应 | file_id | string | 是 | fid，格式 `volume_id,needle_id,cookie` |
| 响应 | volume_id | uint32 | 是 | 从 fid 解析或由响应直接提供 |
| 响应 | volume_url | string | 是 | 写入地址（host:port） |
| 响应 | public_url | string | 否 | 对外地址 |
| 响应 | auth | string | 否 | 写入授权 token（若启用） |

#### 15.8.2 数据面：Volume HTTP PUT/GET

| 接口 | 方法 | 必填输入 | 返回 |
|------|------|----------|------|
| 写入对象 | `PUT /{file_id}` | `body=[]byte`、`Content-Type=application/octet-stream` | `2xx` 视为写入成功 |
| 范围读取 | `GET /{file_id}` + `Range` | `Range: bytes={start}-{end}` | `206` + 字节流 |
| 全量读取 | `GET /{file_id}` | 无 | `200` + 字节流 |

写入失败判定：

- `5xx`、网络超时、连接重置：可重试
- `4xx`（非 408/429）：默认不可重试，转人工或补偿流程

#### 15.8.3 元数据面：Filer.CreateEntry / LookupDirectoryEntry / DeleteEntry

| 接口 | 关键请求字段 | 关键响应字段 | 说明 |
|------|--------------|--------------|------|
| CreateEntry | `parent_dir`、`entry.name`、`entry.is_directory=false`、`entry.chunks[{fid,offset,size}]`、`entry.attributes{mime,file_size,crtime,mtime}` | `error` | 写入成功后注册 key→fid 映射 |
| LookupDirectoryEntry | `parent_dir`、`name` | `entry{chunks,attributes}` | 回放首次读取时获取 fid/chunk |
| DeleteEntry | `parent_dir`、`name`、`recursive` | `error` | 过期清理与手动删除 |
| ListEntries | `parent_dir`、`prefix`、`limit`、`start_from` | `entries[]`、`has_more` | 分页列举目录 |

#### 15.8.4 路由查询：LookupVolume

| 项 | 字段 | 类型 | 说明 |
|----|------|------|------|
| 请求 | volume_ids | []uint32 | 需解析 fid 得到 volume_id |
| 响应 | locations | map[uint32][]Location | 每个 volume 的可用地址列表 |
| Location | url/public_url/data_center/rack | string | 用于就近访问与故障切换 |

#### 15.8.5 存储层内部 DTO（建议）

```go
type AssignParams struct {
    Count       uint32
    Collection  string
    Replication string
    TTLSec      int64
    DataCenter  string
    Rack        string
}

type AssignResult struct {
    FileID    string
    VolumeID  uint32
    VolumeURL string
    PublicURL string
    AuthToken string
}

type EntryMeta struct {
    Key       string
    FileID    string
    VolumeID  uint32
    Size      int64
    MimeType  string
    CreateAt  int64
    ModifyAt  int64
}
```

### 15.9 写入事务与补偿流程（必须实现）

写入状态机：

`Init` → `Assigned` → `DataUploaded` → `EntryCreated` → `Committed`

失败处理规范：

1. `Assign` 失败：直接重试，不产生副作用。
2. `Volume PUT` 失败：可重试同一 `file_id`（最多 3 次）；仍失败则终止并上报。
3. `CreateEntry` 失败：进入补偿队列（带 `file_id + object_key`），后台重试注册。
4. 补偿超过阈值（如 15 分钟）仍失败：标记为 `orphan_suspected`，进入离线清理任务。

幂等约束：

- `object_key` 全局唯一（`camera_id + timestamp + sequence`）。
- 补偿重试 `CreateEntry` 必须按 key 去重，避免重复目录项。

### 15.10 错误码映射与重试策略

| 来源 | 状态 | 动作 | 最大重试 |
|------|------|------|----------|
| gRPC | `UNAVAILABLE` | 指数退避重试 | 3 |
| gRPC | `DEADLINE_EXCEEDED` | 重试并增加超时预算 | 3 |
| gRPC | `RESOURCE_EXHAUSTED` | 降速 + 短暂退避 | 2 |
| gRPC | `INVALID_ARGUMENT` | 立即失败，记录参数错误 | 0 |
| HTTP | `500/502/503/504` | 指数退避重试 | 3 |
| HTTP | `429` | 按 `Retry-After` 或固定退避 | 3 |
| HTTP | `404`（Volume） | 刷新 `volumeCache` 后重试一次 | 1 |
| HTTP | `400/401/403` | 立即失败并告警 | 0 |

统一退避参数建议：

- 初始间隔 `50ms`，倍率 `2.0`，最大间隔 `1s`，总重试窗口不超过 `3s`。

### 15.11 超时预算与降级策略

| 调用 | 超时预算 | 说明 |
|------|----------|------|
| Master.Assign | 300ms | 控制面短调用 |
| Filer.CreateEntry | 300ms | 控制面短调用 |
| Filer.LookupDirectoryEntry | 300ms | 回放首次查元数据 |
| LookupVolume | 200ms | 缓存未命中时触发 |
| Volume PUT | 2s（可按对象大小线性放大） | 数据面主耗时 |
| Volume GET/Range | 2s | 回放读取 |

降级路径：

1. gRPC 控制面连续失败达到阈值（如 30s 内 20 次）时，切换为 `filer_http` 模式。
2. 降级期间继续保留重连探测，探测成功后自动回切 `hybrid`。

### 15.12 观测指标与验收口径

新增指标（Prometheus）：

- `seaweed_assign_latency_ms`（Histogram）
- `seaweed_create_entry_latency_ms`（Histogram）
- `seaweed_volume_put_latency_ms`（Histogram）
- `seaweed_volume_get_range_latency_ms`（Histogram）
- `seaweed_compensation_queue_size`（Gauge）
- `seaweed_compensation_retry_total`（Counter）
- `seaweed_orphan_suspected_total`（Counter）
- `seaweed_hybrid_fallback_total`（Counter）

验收口径：

1. 混合模式稳定运行 24 小时，无未处理补偿积压。
2. `P95` 写入延迟达到 `<3ms` 目标，`P95` 读取延迟达到 `<20ms` 目标。
3. 关闭任一 Master/Filer 节点后，业务无中断且自动恢复。

---

## 十六、Proto 字段映射规范（实现约束）

本节定义 SeaweedFS proto 字段到内部 DTO 的稳定映射，避免上层逻辑直接依赖第三方 proto 结构。

### 16.1 Master.Assign 映射

| 内部字段（AssignParams） | Master.AssignRequest 字段 | 规则 |
|------|------------------------------|------|
| `Count` | `count` | `0` 按 `1` 处理 |
| `Collection` | `collection` | 空值回退 `recordings` |
| `Replication` | `replication` | 空值使用集群默认副本策略 |
| `TTLSec` | `ttl`/`ttl_sec` | 统一转换为秒 |
| `DataCenter` | `data_center` | 可选 |
| `Rack` | `rack` | 可选 |

| Master.AssignResponse 字段 | 内部字段（AssignResult） | 规则 |
|----------------------------|--------------------------|------|
| `fid` | `FileID` | 必填 |
| `url` | `VolumeURL` | 必填，标准化为 `host:port` |
| `public_url` | `PublicURL` | 可选 |
| `auth` | `AuthToken` | 可选 |
| `fid` 解析 | `VolumeID` | 从 `fid` 前缀解析，失败视为协议错误 |

### 16.2 Filer.Entry 映射

| Filer 字段 | 内部字段（EntryMeta） | 规则 |
|-----------|-----------------------|------|
| `name` + `parent_dir` | `Key` | 统一拼接为绝对对象 key |
| `chunks[0].file_id` | `FileID` | 当前 GOP 文件按单 chunk 设计 |
| `chunks[0].size` | `Size` | 若缺失则回退 `attributes.file_size` |
| `attributes.mime` | `MimeType` | 空值回退 `application/octet-stream` |
| `attributes.crtime` | `CreateAt` | 转换为 UnixNano |
| `attributes.mtime` | `ModifyAt` | 转换为 UnixNano |
| `chunks[0].file_id` 解析 | `VolumeID` | 解析失败返回 `ErrInvalidFID` |

### 16.3 LookupVolume 映射

| LookupVolume 响应 | 内部字段 | 规则 |
|------------------|----------|------|
| `locations[volume_id][]` | `[]VolumeLocation` | 仅保留可直连地址 |
| `location.url/public_url` | `Addr` | 优先内网地址，失败回退 public_url |
| `location.data_center` | `DataCenter` | 用于本地优先路由 |
| `location.rack` | `Rack` | 同机架优先 |

### 16.4 fid 解析规范

```text
fid format: {volume_id},{needle_id},{cookie}
example:    3,01637037d6
```

解析规则：

1. 先按首个 `,` 切分并解析 `volume_id`（十进制）。
2. `volume_id` 必须大于 `0`。
3. 非法 fid 统一返回 `ErrInvalidFID`，禁止继续发起 Volume 请求。

---

## 十七、存储层代码骨架（可直接开工）

### 17.1 文件清单与职责

| 文件 | 职责 |
|------|------|
| `internal/storage/interface.go` | `StorageBackend`、通用错误定义 |
| `internal/storage/seaweed_hybrid.go` | `Put/Get/GetRange/Delete/List` 主流程编排 |
| `internal/storage/grpc_master_client.go` | `Assign/LookupVolume` 包装与连接池 |
| `internal/storage/grpc_filer_client.go` | `CreateEntry/LookupEntry/Delete/List` 包装 |
| `internal/storage/http_volume_client.go` | `PUT/GET/Range` 与连接池、超时控制 |
| `internal/storage/fid_pool.go` | fid 批量申请、低水位补充 |
| `internal/storage/volume_cache.go` | volume_id 路由缓存 |
| `internal/storage/entry_cache.go` | object_key 元数据缓存 |
| `internal/storage/compensation_queue.go` | `CreateEntry` 失败补偿队列 |
| `internal/storage/seaweed_filer_http.go` | 降级实现（`filer_http`） |

### 17.2 建议接口签名

```go
type MasterClient interface {
    Assign(ctx context.Context, p AssignParams) (AssignResult, error)
    LookupVolume(ctx context.Context, volumeIDs []uint32) (map[uint32][]VolumeLocation, error)
}

type FilerClient interface {
    CreateEntry(ctx context.Context, meta EntryMeta) error
    LookupEntry(ctx context.Context, key string) (EntryMeta, error)
    DeleteEntry(ctx context.Context, key string, recursive bool) error
    ListEntries(ctx context.Context, prefix string, limit int, startFrom string) ([]EntryMeta, bool, error)
}

type VolumeClient interface {
    Put(ctx context.Context, volumeURL, fileID string, data []byte, authToken string) error
    Get(ctx context.Context, volumeURL, fileID string) ([]byte, error)
    GetRange(ctx context.Context, volumeURL, fileID string, offset, length int64) ([]byte, error)
}
```

### 17.3 Put 主流程伪代码

```go
func (s *SeaweedHybridStorage) Put(ctx context.Context, key string, data []byte) error {
    a, err := s.fidPool.Get(ctx)
    if err != nil {
        return err
    }

    if err := s.volume.Put(ctx, a.VolumeURL, a.FileID, data, a.AuthToken); err != nil {
        return classifyRetryable(err)
    }

    meta := EntryMeta{
        Key: key, FileID: a.FileID, VolumeID: a.VolumeID,
        Size: int64(len(data)), MimeType: "video/mp4",
        CreateAt: time.Now().UnixNano(), ModifyAt: time.Now().UnixNano(),
    }
    if err := s.filer.CreateEntry(ctx, meta); err != nil {
        s.compensation.Enqueue(meta)
        return ErrEntryPendingCompensation
    }
    return nil
}
```

### 17.4 统一错误定义（建议）

```go
var (
    ErrInvalidFID              = errors.New("invalid fid")
    ErrVolumeRouteNotFound     = errors.New("volume route not found")
    ErrEntryNotFound           = errors.New("entry not found")
    ErrEntryPendingCompensation = errors.New("entry pending compensation")
    ErrStorageDegraded         = errors.New("storage degraded to filer_http")
)
```

---

## 十八、两周落地任务拆解（工程化）

### 18.1 Week 1（能力建设）

1. 完成 `MasterClient/FilerClient/VolumeClient` 基础实现与单元测试。
2. 完成 `fid_pool`、`volume_cache`、`entry_cache`。
3. 完成 `SeaweedHybridStorage.Put/GetRange` 主链路。
4. 接入基础指标：`assign/create_entry/put/get_range` 延迟与错误计数。

### 18.2 Week 2（可靠性与验收）

1. 完成补偿队列与离线孤儿扫描任务。
2. 完成 `filer_http` 降级与自动回切机制。
3. 完成故障注入测试（Master/Filer 节点下线、Volume 404、网络抖动）。
4. 完成压测并输出 `P50/P95/P99` 报告。

### 18.3 DoD（完成定义）

1. 功能：`Put/GetRange/Delete/List` 在混合模式下可用，降级模式可用。
2. 可靠性：补偿队列无长期堆积，故障恢复后自动清空。
3. 性能：达到第 15.6 节目标或给出偏差解释与优化计划。
4. 可观测：第 15.12 节指标完整、可用于告警。

---

## 十九、故障注入与压测用例清单

### 19.1 故障注入矩阵（必须覆盖）

| 用例ID | 场景 | 注入方式 | 期望行为 | 验收指标 |
|------|------|----------|----------|----------|
| FI-01 | Master 单节点下线 | 关闭一个 Master gRPC 端点 | `Assign` 自动切换到其他节点 | 写入不中断；错误峰值恢复 < 30s |
| FI-02 | Filer 单节点下线 | 关闭一个 Filer gRPC 端点 | `CreateEntry/LookupEntry` 自动切换 | 回放不中断；`fallback_total` 不持续增长 |
| FI-03 | 所有 gRPC 不可达 | 阻断 19333/18888 端口 | 自动降级到 `filer_http` | 降级成功率 100%；业务可持续写入 |
| FI-04 | gRPC 恢复 | 解除端口阻断 | 自动回切 `hybrid` | 回切后 5 分钟内稳定，无抖动切换 |
| FI-05 | Volume 返回 404 | 读取旧路由后请求 | 刷新 `volumeCache` 并重试一次 | 重试成功率 > 99% |
| FI-06 | Volume 5xx 抖动 | 注入 5% 503 | 指数退避重试，失败可观测 | 最终失败率 < 0.5% |
| FI-07 | CreateEntry 连续失败 | 模拟 Filer 超时 | 进入补偿队列 | 队列可消费，最终孤儿率接近 0 |
| FI-08 | 网络时延上升 | 增加 100-300ms RTT | 系统维持可用，延迟可观测 | P95 退化可解释，无雪崩 |
| FI-09 | 录制高压+故障叠加 | 1000 路写入时注入 FI-01/06 | 触发背压但不 OOM | 进程稳定，无崩溃 |
| FI-10 | 回放高压+故障叠加 | 500 并发回放时注入 FI-02/05 | 回放可降级但不中断 | 首帧与 seek 退化在阈值内 |

### 19.2 压测模型（建议基线）

录制压测：

- 路数：`200/500/1000`
- 码率：`2/4/8 Mbps`
- GOP：`2s`
- 写入对象：fMP4 GOP fragment（0.5-2MB）

回放压测：

- 并发会话：`100/300/500`
- 读模式：`顺序播放` + `随机 seek`（每 5-10s 一次）
- Range 读取比例：`>90%`

混合场景压测：

- 录制 1000 路 + 回放 300 会话并行
- 同时执行定时删除任务（模拟真实运维窗口）

### 19.3 压测输出模板（固定产物）

每轮压测必须输出以下指标：

1. 吞吐：`put_qps`、`get_range_qps`、`delete_qps`
2. 延迟：`P50/P95/P99`（Assign、CreateEntry、PUT、GET/Range、Seek）
3. 可靠性：失败率、重试次数、补偿队列峰值、孤儿对象数
4. 资源：CPU、内存、FD、网络带宽、GC 停顿
5. 降级：降级触发次数、持续时长、回切成功率

### 19.4 通过门槛（Go/No-Go）

| 维度 | 门槛 |
|------|------|
| 功能正确性 | 全部 P0 用例通过，且无数据一致性错误 |
| 写入延迟 | `P95 < 3ms`（稳态，非故障窗口） |
| 读取延迟 | `P95 < 20ms`（稳态，非故障窗口） |
| Seek 体验 | 请求到首帧 `< 200ms`（P95） |
| 稳定性 | 24h 压测无崩溃，无不可恢复积压 |
| 补偿治理 | 孤儿对象率 `< 0.01%` |

---

## 二十、发布前检查清单（Runbook）

### 20.1 配置检查

1. `storage.seaweedfs.mode=hybrid` 且 gRPC 地址配置完整。
2. `fid_pool.batch_assign_count/low_watermark` 已按规模调优。
3. `volume_ttl_seconds`、`entry_lru_size` 与内存预算一致。
4. mTLS/JWT（如启用）证书和 token 有效期覆盖发布窗口。

### 20.2 连通性检查

1. 录制节点可访问 Master `19333`、Filer `18888`、Volume `8080/18080`。
2. 对每个端点执行健康探测（启动前和发布后各一次）。
3. 人工验证 `hybrid -> filer_http -> hybrid` 切换链路。

### 20.3 观测与告警检查

1. 第 15.12 节新增指标全部可见。
2. 告警规则已启用：高错误率、补偿队列积压、降级持续超阈值。
3. 仪表盘包含录制、回放、存储控制面与数据面四个视图。

### 20.4 回滚策略

1. 支持配置热切换到 `filer_http`。
2. 保留最近稳定版本镜像，可在 10 分钟内回滚。
3. 回滚后执行补偿队列一致性核对，避免遗留孤儿对象。
