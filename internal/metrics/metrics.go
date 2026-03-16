package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// Recording metrics
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

	MemoryPressureLevel = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "memory_pressure_level",
		Help: "Current memory pressure level (0-3)",
	})

	// SeaweedFS storage-layer metrics (§15.12)
	SeaweedAssignLatency = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "seaweed_assign_latency_seconds",
		Help:    "SeaweedFS Master.Assign latency distribution",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 10),
	})

	SeaweedCreateEntryLatency = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "seaweed_create_entry_latency_seconds",
		Help:    "SeaweedFS Filer.CreateEntry latency distribution",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 10),
	})

	SeaweedVolumePutLatency = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "seaweed_volume_put_latency_seconds",
		Help:    "SeaweedFS Volume HTTP PUT latency distribution",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 10),
	})

	SeaweedVolumeGetRangeLatency = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "seaweed_volume_get_range_latency_seconds",
		Help:    "SeaweedFS Volume HTTP GET/Range latency distribution",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 10),
	})

	SeaweedCompensationQueueSize = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "seaweed_compensation_queue_size",
		Help: "Current size of the CreateEntry compensation queue",
	})

	SeaweedCompensationRetryTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "seaweed_compensation_retry_total",
		Help: "Total number of CreateEntry compensation retries",
	})

	SeaweedOrphanSuspectedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "seaweed_orphan_suspected_total",
		Help: "Total number of suspected orphan objects (compensation exceeded threshold)",
	})

	SeaweedHybridFallbackTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "seaweed_hybrid_fallback_total",
		Help: "Total number of fallbacks from hybrid to filer_http mode",
	})
)
