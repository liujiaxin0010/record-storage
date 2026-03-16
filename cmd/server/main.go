package main

import (
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"recording-server/internal/api"
	"recording-server/internal/config"
	"recording-server/internal/index"
	"recording-server/internal/playback"
	"recording-server/internal/puller"
	"recording-server/internal/recorder"
	"recording-server/internal/storage"
)

func main() {
	validateOnly := flag.Bool("validate", false, "validate config and exit")
	flag.Parse()

	configPath := "config.yaml"
	if flag.NArg() > 0 {
		configPath = flag.Arg(0)
	}

	cfg, err := config.Load(configPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	if *validateOnly {
		log.Printf("✓ Configuration is valid")
		log.Printf("  - %d cameras configured", len(cfg.Cameras))
		log.Printf("  - RTSP playback port: %d", cfg.Server.RTSPPlaybackPort)
		log.Printf("  - Async writer: %d workers, queue size %d", cfg.Recorder.AsyncWriterWorkers, cfg.Recorder.AsyncWriterQueueSize)
		os.Exit(0)
	}

	if err := os.MkdirAll(filepath.Dir(cfg.Index.BoltPath), 0o755); err != nil {
		log.Fatalf("create data dir: %v", err)
	}

	var store storage.StorageBackend
	if cfg.Storage.SeaweedFS.Mode == "hybrid" {
		store, err = storage.NewSeaweedHybridStorage(cfg.Storage.SeaweedFS)
	} else {
		store = storage.NewFilerHTTPStorage(cfg.Storage.SeaweedFS.FilerHTTPURL)
	}
	if err != nil {
		log.Fatalf("init storage: %v", err)
	}
	defer store.Close()

	idx, err := index.NewBoltIndex(cfg.Index.BoltPath)
	if err != nil {
		log.Fatalf("init index: %v", err)
	}
	defer idx.Close()

	writer := recorder.NewAsyncWriter(store, idx, cfg.Recorder.AsyncWriterWorkers, cfg.Recorder.AsyncWriterQueueSize)
	defer writer.Close()

	playbackServer := playback.NewServer(cfg.Server.RTSPPlaybackPort, store, idx)
	if err := playbackServer.Start(); err != nil {
		log.Fatalf("start playback server: %v", err)
	}
	defer playbackServer.Close()

	recorders := make(map[string]*recorder.StreamRecorder)
	var pullers []*puller.RTSPPuller
	for _, camera := range cfg.Cameras {
		if !camera.Enabled {
			continue
		}
		streamRecorder := recorder.NewStreamRecorder(camera.ID, writer)
		recorders[camera.ID] = streamRecorder
		if camera.Recording.AutoStart && len(camera.Recording.Schedule) == 0 {
			streamRecorder.Start()
		}
		rtspPuller := puller.NewRTSPPuller(camera.ID, camera.RTSPURL, camera.Transport, streamRecorder.ProcessSample, func(event string, eventErr error) {
			metadata := map[string]string{}
			if eventErr != nil {
				metadata["error"] = eventErr.Error()
			}
			_ = writer.WriteEvent(camera.ID, event, metadata)
		})
		if err := rtspPuller.Start(); err != nil {
			log.Printf("skip camera %s: %v", camera.ID, err)
			continue
		}
		pullers = append(pullers, rtspPuller)
		log.Printf("camera %s started", camera.ID)
	}

	scheduleController := recorder.NewScheduleController(cfg.Cameras, recorders)
	scheduleController.Start()
	defer scheduleController.Stop()

	go http.ListenAndServe(":9090", promhttp.Handler())

	apiServer := api.NewServer(8080, idx, writer, recorders)
	if err := apiServer.Start(); err != nil {
		log.Printf("warning: failed to start API server: %v", err)
	}
	defer apiServer.Close()

	log.Printf("server ready: %d active camera(s), playback on rtsp://0.0.0.0:%d/playback/{camera_id}?start=RFC3339", len(pullers), cfg.Server.RTSPPlaybackPort)
	log.Printf("metrics: http://0.0.0.0:9090/metrics, api: http://0.0.0.0:8080/api/v1/")

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Printf("shutting down...")
	for _, item := range pullers {
		item.Stop()
	}
	scheduleController.Stop()
	playbackServer.Close()
	writer.Close()
	idx.Close()
	store.Close()
	log.Printf("shutdown complete")
}
