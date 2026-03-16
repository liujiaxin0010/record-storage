package config

import (
	"errors"
	"fmt"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

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
	MaxMemoryMB       int `yaml:"max_memory_mb"`
	GOPBufferPoolSize int `yaml:"gop_buffer_pool_size"`
}

type StorageConfig struct {
	SeaweedFS SeaweedFSConfig `yaml:"seaweedfs"`
}

type SeaweedFSConfig struct {
	Mode             string              `yaml:"mode"`
	FilerHTTPURL     string              `yaml:"filer_http_url"`
	MasterGRPCAddrs  []string            `yaml:"master_grpc_addrs"`
	FilerGRPCAddrs   []string            `yaml:"filer_grpc_addrs"`
	VolumeHTTPScheme string              `yaml:"volume_http_scheme"`
	GRPC             SeaweedGRPCConfig   `yaml:"grpc"`
	FIDPool          SeaweedFIDPool      `yaml:"fid_pool"`
	Cache            SeaweedCacheConfig  `yaml:"cache"`
	Security         SeaweedSecurity     `yaml:"security"`
}

type SeaweedGRPCConfig struct {
	KeepaliveSeconds int `yaml:"keepalive_seconds"`
	MaxRecvMsgMB     int `yaml:"max_recv_msg_mb"`
	MaxSendMsgMB     int `yaml:"max_send_msg_mb"`
	MasterPoolSize   int `yaml:"master_pool_size"`
	FilerPoolSize    int `yaml:"filer_pool_size"`
}

type SeaweedFIDPool struct {
	BatchAssignCount int `yaml:"batch_assign_count"`
	LowWatermark     int `yaml:"low_watermark"`
}

type SeaweedCacheConfig struct {
	VolumeTTLSeconds int `yaml:"volume_ttl_seconds"`
	EntryLRUSize     int `yaml:"entry_lru_size"`
	EntryTTLSeconds  int `yaml:"entry_ttl_seconds"`
}

type SeaweedSecurity struct {
	EnableMTLS bool   `yaml:"enable_mtls"`
	CAFile     string `yaml:"ca_file"`
	CertFile   string `yaml:"cert_file"`
	KeyFile    string `yaml:"key_file"`
	JWTToken   string `yaml:"jwt_token"`
}

type IndexConfig struct {
	BoltPath         string `yaml:"bolt_path"`
	BatchIntervalMS  int    `yaml:"batch_interval_ms"`
	SnapshotInterval int    `yaml:"snapshot_interval"`
}

type RecorderConfig struct {
	SegmentMode          string `yaml:"segment_mode"`
	ChunkMaxGOPs         int    `yaml:"chunk_max_gops"`
	AsyncWriterWorkers   int    `yaml:"async_writer_workers"`
	AsyncWriterQueueSize int    `yaml:"async_writer_queue_size"`
	RetryMax             int    `yaml:"retry_max"`
	RetentionDays        int    `yaml:"retention_days"`
}

type CameraConfig struct {
	ID        string          `yaml:"id"`
	RTSPURL   string          `yaml:"rtsp_url"`
	Transport string          `yaml:"transport"`
	Enabled   bool            `yaml:"enabled"`
	Recording RecordingConfig `yaml:"recording"`
}

type RecordingConfig struct {
	AutoStart bool             `yaml:"auto_start"`
	Schedule  []ScheduleConfig `yaml:"schedule"`
}

type ScheduleConfig struct {
	Weekdays []int  `yaml:"weekdays"`
	Start    string `yaml:"start"`
	End      string `yaml:"end"`
}

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}

	cfg.applyDefaults(path)
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("validate config: %w", err)
	}

	return &cfg, nil
}

func (c *Config) applyDefaults(configPath string) {
	if c.Server.RTSPPlaybackPort == 0 {
		c.Server.RTSPPlaybackPort = 8554
	}
	if c.Server.APIPort == 0 {
		c.Server.APIPort = 8080
	}
	if c.Server.MetricsPort == 0 {
		c.Server.MetricsPort = 9090
	}
	if c.Memory.MaxMemoryMB == 0 {
		c.Memory.MaxMemoryMB = 4096
	}
	if c.Memory.GOPBufferPoolSize == 0 {
		c.Memory.GOPBufferPoolSize = 2 * 1024 * 1024
	}
	if c.Storage.SeaweedFS.Mode == "" {
		c.Storage.SeaweedFS.Mode = "hybrid"
	}
	if c.Storage.SeaweedFS.VolumeHTTPScheme == "" {
		c.Storage.SeaweedFS.VolumeHTTPScheme = "http"
	}
	if c.Storage.SeaweedFS.GRPC.KeepaliveSeconds == 0 {
		c.Storage.SeaweedFS.GRPC.KeepaliveSeconds = 30
	}
	if c.Storage.SeaweedFS.GRPC.MaxRecvMsgMB == 0 {
		c.Storage.SeaweedFS.GRPC.MaxRecvMsgMB = 4
	}
	if c.Storage.SeaweedFS.GRPC.MaxSendMsgMB == 0 {
		c.Storage.SeaweedFS.GRPC.MaxSendMsgMB = 4
	}
	if c.Storage.SeaweedFS.FIDPool.BatchAssignCount == 0 {
		c.Storage.SeaweedFS.FIDPool.BatchAssignCount = 100
	}
	if c.Storage.SeaweedFS.FIDPool.LowWatermark == 0 {
		c.Storage.SeaweedFS.FIDPool.LowWatermark = 20
	}
	if c.Storage.SeaweedFS.Cache.VolumeTTLSeconds == 0 {
		c.Storage.SeaweedFS.Cache.VolumeTTLSeconds = 300
	}
	if c.Storage.SeaweedFS.Cache.EntryTTLSeconds == 0 {
		c.Storage.SeaweedFS.Cache.EntryTTLSeconds = 120
	}
	if c.Storage.SeaweedFS.Cache.EntryLRUSize == 0 {
		c.Storage.SeaweedFS.Cache.EntryLRUSize = 10000
	}
	if c.Index.BoltPath == "" {
		c.Index.BoltPath = filepath.Join(filepath.Dir(configPath), "data", "recordings.db")
	}
	if c.Index.BatchIntervalMS == 0 {
		c.Index.BatchIntervalMS = 100
	}
	if c.Index.SnapshotInterval == 0 {
		c.Index.SnapshotInterval = 300
	}
	if c.Recorder.SegmentMode == "" {
		c.Recorder.SegmentMode = "gop"
	}
	if c.Recorder.ChunkMaxGOPs == 0 {
		c.Recorder.ChunkMaxGOPs = 15
	}
	if c.Recorder.AsyncWriterWorkers == 0 {
		c.Recorder.AsyncWriterWorkers = 4
	}
	if c.Recorder.AsyncWriterQueueSize == 0 {
		c.Recorder.AsyncWriterQueueSize = 256
	}
	if c.Recorder.RetryMax == 0 {
		c.Recorder.RetryMax = 3
	}
	if c.Recorder.RetentionDays == 0 {
		c.Recorder.RetentionDays = 30
	}
	for index := range c.Cameras {
		if c.Cameras[index].Transport == "" {
			c.Cameras[index].Transport = "auto"
		}
	}
}

func (c *Config) Validate() error {
	if err := validatePort(c.Server.RTSPPlaybackPort, "rtsp_playback_port"); err != nil {
		return err
	}
	if err := validatePort(c.Server.APIPort, "api_port"); err != nil {
		return err
	}
	if err := validatePort(c.Server.MetricsPort, "metrics_port"); err != nil {
		return err
	}

	mode := strings.ToLower(strings.TrimSpace(c.Storage.SeaweedFS.Mode))
	if mode != "hybrid" && mode != "filer_http" {
		return fmt.Errorf("invalid storage mode: %s", c.Storage.SeaweedFS.Mode)
	}
	if c.Storage.SeaweedFS.FilerHTTPURL == "" {
		return errors.New("filer_http_url is required")
	}
	if _, err := url.Parse(c.Storage.SeaweedFS.FilerHTTPURL); err != nil {
		return fmt.Errorf("invalid filer_http_url: %w", err)
	}
	if mode == "hybrid" {
		if len(c.Storage.SeaweedFS.MasterGRPCAddrs) == 0 {
			return errors.New("master_grpc_addrs is required in hybrid mode")
		}
		if len(c.Storage.SeaweedFS.FilerGRPCAddrs) == 0 {
			return errors.New("filer_grpc_addrs is required in hybrid mode")
		}
	}
	if c.Index.BoltPath == "" {
		return errors.New("bolt_path is required")
	}
	if c.Recorder.AsyncWriterWorkers <= 0 {
		return errors.New("async_writer_workers must be > 0")
	}
	if c.Recorder.AsyncWriterQueueSize <= 0 {
		return errors.New("async_writer_queue_size must be > 0")
	}
	if c.Recorder.SegmentMode != "gop" && c.Recorder.SegmentMode != "chunk" {
		return fmt.Errorf("invalid segment_mode: %s", c.Recorder.SegmentMode)
	}
	if len(c.Cameras) == 0 {
		return errors.New("no cameras configured")
	}

	seen := make(map[string]struct{}, len(c.Cameras))
	for _, camera := range c.Cameras {
		if strings.TrimSpace(camera.ID) == "" {
			return errors.New("camera id is required")
		}
		if _, ok := seen[camera.ID]; ok {
			return fmt.Errorf("duplicate camera id: %s", camera.ID)
		}
		seen[camera.ID] = struct{}{}
		if err := validateRTSPURL(camera.RTSPURL); err != nil {
			return fmt.Errorf("camera %s: %w", camera.ID, err)
		}
		transport := strings.ToLower(strings.TrimSpace(camera.Transport))
		if transport != "auto" && transport != "tcp" && transport != "udp" {
			return fmt.Errorf("camera %s: invalid transport %q", camera.ID, camera.Transport)
		}
		for _, schedule := range camera.Recording.Schedule {
			if _, err := time.Parse("15:04", schedule.Start); err != nil {
				return fmt.Errorf("camera %s: invalid schedule start %q", camera.ID, schedule.Start)
			}
			if _, err := time.Parse("15:04", schedule.End); err != nil {
				return fmt.Errorf("camera %s: invalid schedule end %q", camera.ID, schedule.End)
			}
		}
	}

	// Performance validation and warnings
	if c.Recorder.AsyncWriterQueueSize < c.Recorder.AsyncWriterWorkers*10 {
		log.Printf("WARNING: queue_size (%d) is small relative to workers (%d), may cause blocking",
			c.Recorder.AsyncWriterQueueSize, c.Recorder.AsyncWriterWorkers)
	}

	numCPU := runtime.NumCPU()
	if c.Recorder.AsyncWriterWorkers > numCPU*2 {
		log.Printf("WARNING: async_writer_workers (%d) exceeds 2x CPU cores (%d), may cause contention",
			c.Recorder.AsyncWriterWorkers, numCPU)
	}

	enabledCameras := 0
	for _, cam := range c.Cameras {
		if cam.Enabled {
			enabledCameras++
		}
	}

	estimatedMemoryMB := enabledCameras * 10 // ~10MB per camera estimate
	if estimatedMemoryMB > c.Memory.MaxMemoryMB {
		log.Printf("WARNING: %d cameras may use ~%dMB, exceeds max_memory_mb (%dMB)",
			enabledCameras, estimatedMemoryMB, c.Memory.MaxMemoryMB)
	}

	return nil
}

func (c *Config) GetBatchInterval() time.Duration {
	return time.Duration(c.Index.BatchIntervalMS) * time.Millisecond
}

func validatePort(port int, name string) error {
	if port <= 0 || port > 65535 {
		return fmt.Errorf("invalid %s: %d", name, port)
	}
	return nil
}

func validateRTSPURL(raw string) error {
	if raw == "" {
		return errors.New("rtsp_url is required")
	}
	u, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("invalid rtsp_url: %w", err)
	}
	if u.Scheme != "rtsp" && u.Scheme != "rtsps" {
		return fmt.Errorf("unsupported rtsp_url scheme: %s", u.Scheme)
	}
	if u.Host == "" {
		return errors.New("rtsp_url host is required")
	}
	return nil
}
