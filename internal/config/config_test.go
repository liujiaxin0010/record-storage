package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadHybridConfig(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.yaml")
	content := `server:
  rtsp_playback_port: 8554
storage:
  seaweedfs:
    mode: "hybrid"
    filer_http_url: "http://localhost:8888"
    master_grpc_addrs: ["localhost:19333"]
    filer_grpc_addrs: ["localhost:18888"]
index:
  bolt_path: "` + filepath.ToSlash(filepath.Join(tmpDir, "recordings.db")) + `"
recorder:
  async_writer_workers: 2
  async_writer_queue_size: 8
cameras:
  - id: "cam01"
    rtsp_url: "rtsp://user:pass@localhost:554/stream"
    transport: "tcp"
    enabled: true
    recording:
      auto_start: true
`
	if err := os.WriteFile(configPath, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(configPath)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.Storage.SeaweedFS.Mode != "hybrid" {
		t.Fatalf("unexpected mode: %s", cfg.Storage.SeaweedFS.Mode)
	}
	if cfg.Cameras[0].Transport != "tcp" {
		t.Fatalf("unexpected transport: %s", cfg.Cameras[0].Transport)
	}
}

func TestValidateDuplicateCameraID(t *testing.T) {
	cfg := &Config{
		Server: ServerConfig{RTSPPlaybackPort: 8554, APIPort: 8080, MetricsPort: 9090},
		Storage: StorageConfig{SeaweedFS: SeaweedFSConfig{Mode: "filer_http", FilerHTTPURL: "http://localhost:8888"}},
		Index: IndexConfig{BoltPath: "test.db"},
		Recorder: RecorderConfig{SegmentMode: "gop", AsyncWriterWorkers: 1, AsyncWriterQueueSize: 1},
		Cameras: []CameraConfig{
			{ID: "cam01", RTSPURL: "rtsp://localhost:554/a", Transport: "tcp", Enabled: true},
			{ID: "cam01", RTSPURL: "rtsp://localhost:554/b", Transport: "udp", Enabled: true},
		},
	}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected duplicate camera validation error")
	}
}
