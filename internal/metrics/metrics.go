package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	RecordingStreams = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "recording_streams_active",
		Help: "Number of active recording streams",
	})

	PlaybackSessions = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "playback_sessions_active",
		Help: "Number of active playback sessions",
	})

	GOPsWritten = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "gops_written_total",
		Help: "Total number of GOPs written",
	}, []string{"camera_id"})

	WriterQueueDepth = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "writer_queue_depth",
		Help: "Current async writer queue depth",
	})

	WriterDropped = promauto.NewCounter(prometheus.CounterOpts{
		Name: "writer_dropped_total",
		Help: "Total number of dropped write jobs",
	})

	StorageWriteLatency = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "storage_write_latency_seconds",
		Help:    "Storage write latency distribution",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 10),
	})

	IndexQueryLatency = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "index_query_latency_seconds",
		Help:    "Index query latency distribution",
		Buckets: prometheus.ExponentialBuckets(0.0001, 2, 10),
	})

	MemoryUsageBytes = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "memory_usage_bytes",
		Help: "Current memory usage in bytes",
	})
)
