// Package metrics defines all Prometheus metrics and the metrics HTTP server.
//
// Metric categories (22 metrics total):
//
//   Throughput (4):
//     scylla2ch_rows_consumed_total{table}   — CDC rows read from ScyllaDB
//     scylla2ch_rows_flushed_total{table}    — rows written to ClickHouse
//     scylla2ch_rows_dropped_total{table}    — rows lost after retry exhaustion (ALERT)
//     scylla2ch_rows_dlq_total{table}        — poison rows sent to DLQ file (ALERT)
//
//   Flush operations (3):
//     scylla2ch_flushes_total{table,status}     — flush attempts (ok/error)
//     scylla2ch_flush_duration_seconds{table}    — flush latency histogram
//     scylla2ch_flush_batch_size{table}          — rows per flush histogram
//
//   Buffer / back-pressure (1):
//     scylla2ch_buffer_rows{table}  — current channel buffer depth
//
//   CDC deletes (1):
//     scylla2ch_deletes_skipped_total{table} — CDC delete events skipped
//
//   CDC consumer (2):
//     scylla2ch_consumers_active    — active stream consumers
//     scylla2ch_cdc_lag_seconds{table} — replication lag (time since CDC event)
//
//   Retry (1):
//     scylla2ch_retries_total{table}  — ClickHouse retry attempts
//
//   Hot-reload (2):
//     scylla2ch_tables_active        — tables being replicated
//     scylla2ch_hot_reload_total     — tables hot-reloaded without restart
//
//   Error tracking (2):
//     scylla2ch_errors_total{component,severity}  — errors by component and severity
//     scylla2ch_reader_errors_total{table}         — per-table reader errors
//
//   Last-seen timestamps (2):
//     scylla2ch_last_consumed_timestamp_ms{table} — Unix ms when last CDC row was consumed
//     scylla2ch_last_flushed_timestamp_ms{table}  — Unix ms when last batch was flushed to CH
//
//   Connection health (3):
//     scylla2ch_scylla_connected      — 1 if connected, 0 if not
//     scylla2ch_clickhouse_connected  — 1 if connected, 0 if not
//     scylla2ch_uptime_seconds        — seconds since start
//
//   Info (1):
//     scylla2ch_build_info{version,go_version}  — build metadata
//
// Alert rules (recommended):
//   scylla2ch_rows_dropped_total > 0          → data loss, page immediately
//   scylla2ch_cdc_lag_seconds > 300           → replication falling behind
//   scylla2ch_errors_total{severity="critical"} > 0  → flush exhaustion
//   scylla2ch_buffer_rows > batch_size * 1.5  → back-pressure building up
package metrics

import (
	"fmt"
	"net/http"
	"runtime"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"scylla2ch/config"
)

var (
	// --- Throughput ---

	RowsConsumed = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "scylla2ch_rows_consumed_total",
			Help: "Total CDC rows consumed from ScyllaDB, by table",
		},
		[]string{"table"},
	)

	RowsFlushed = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "scylla2ch_rows_flushed_total",
			Help: "Total rows flushed to ClickHouse, by table",
		},
		[]string{"table"},
	)

	RowsDropped = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "scylla2ch_rows_dropped_total",
			Help: "Total rows dropped after exhausting retries (data loss), by table",
		},
		[]string{"table"},
	)

	RowsDLQ = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "scylla2ch_rows_dlq_total",
			Help: "Total poison rows sent to DLQ (structural Append errors), by table",
		},
		[]string{"table"},
	)

	// --- Flush operations ---

	FlushesTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "scylla2ch_flushes_total",
			Help: "Total flush attempts to ClickHouse, by table and status (ok/error)",
		},
		[]string{"table", "status"},
	)

	FlushLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "scylla2ch_flush_duration_seconds",
			Help:    "Histogram of flush duration to ClickHouse, by table",
			Buckets: []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30},
		},
		[]string{"table"},
	)

	FlushBatchSize = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "scylla2ch_flush_batch_size",
			Help:    "Number of rows per flush batch, by table",
			Buckets: []float64{1, 10, 50, 100, 500, 1000, 2500, 5000, 10000, 25000},
		},
		[]string{"table"},
	)

	// --- Buffer / back-pressure ---

	BufferSize = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "scylla2ch_buffer_rows",
			Help: "Current number of rows in the channel buffer, by table",
		},
		[]string{"table"},
	)

	// --- CDC deletes (intentionally skipped) ---

	DeletesSkipped = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "scylla2ch_deletes_skipped_total",
			Help: "Total CDC delete events skipped (RowDelete + PartitionDelete), by table",
		},
		[]string{"table"},
	)

	// --- CDC consumer ---

	ConsumersActive = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "scylla2ch_consumers_active",
			Help: "Number of active CDC stream consumers",
		},
	)

	CDCLagSeconds = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "scylla2ch_cdc_lag_seconds",
			Help: "Replication lag: time since the latest CDC event timestamp, by table",
		},
		[]string{"table"},
	)

	// --- Retry ---

	RetriesTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "scylla2ch_retries_total",
			Help: "Total retry attempts for ClickHouse flushes, by table",
		},
		[]string{"table"},
	)

	// --- Hot-reload ---

	TablesActive = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "scylla2ch_tables_active",
			Help: "Number of tables currently being replicated",
		},
	)

	HotReloadTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "scylla2ch_hot_reload_total",
			Help: "Total number of tables hot-reloaded (detected and started without restart)",
		},
	)

	SighupReloadTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "scylla2ch_sighup_reload_total",
			Help: "Total number of config reloads triggered by SIGHUP signal",
		},
	)

	// --- Error tracking (for Sentry/Grafana alerts) ---

	ErrorsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "scylla2ch_errors_total",
			Help: "Total errors by component and severity (for alerting)",
		},
		[]string{"component", "severity"},
		// component: "reader", "writer", "discovery", "connection"
		// severity: "warning", "error", "critical"
	)

	ReaderErrorsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "scylla2ch_reader_errors_total",
			Help: "Total CDC reader errors, by table",
		},
		[]string{"table"},
	)

	// --- Connection health ---

	ScyllaConnected = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "scylla2ch_scylla_connected",
			Help: "1 if connected to ScyllaDB, 0 if not",
		},
	)

	ClickHouseConnected = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "scylla2ch_clickhouse_connected",
			Help: "1 if connected to ClickHouse, 0 if not",
		},
	)

	UptimeSeconds = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "scylla2ch_uptime_seconds",
			Help: "Seconds since scylla2ch started",
		},
	)

	// --- Last-seen timestamps (for dashboard staleness) ---

	LastConsumedTimestampMs = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "scylla2ch_last_consumed_timestamp_ms",
			Help: "Unix ms when last CDC row was consumed from ScyllaDB, by table",
		},
		[]string{"table"},
	)

	LastFlushedTimestampMs = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "scylla2ch_last_flushed_timestamp_ms",
			Help: "Unix ms when last batch was flushed to ClickHouse, by table",
		},
		[]string{"table"},
	)

	// --- CDC gap detection ---

	CDCGapSeconds = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "scylla2ch_cdc_gap_seconds",
			Help: "Seconds of CDC data skipped due to log TTL expiry (>0 means data loss from extended downtime)",
		},
		[]string{"table"},
	)

	// --- DLQ / checkpoint observability ---

	DLQBytes = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "scylla2ch_dlq_bytes",
			Help: "DLQ file size in bytes, by table (detect schema drift accumulation)",
		},
		[]string{"table"},
	)

	CheckpointErrorsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "scylla2ch_checkpoint_errors_total",
			Help: "Total checkpoint save failures, by table",
		},
		[]string{"table"},
	)

	// --- Runtime (goroutine/memory leak detection) ---

	GoroutineCount = prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Name: "scylla2ch_goroutines",
			Help: "Current number of goroutines (detect leaks)",
		},
		func() float64 { return float64(runtime.NumGoroutine()) },
	)

	HeapAllocBytes = prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Name: "scylla2ch_heap_alloc_bytes",
			Help: "Current heap allocation in bytes (detect memory growth)",
		},
		func() float64 {
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			return float64(m.HeapAlloc)
		},
	)

	// --- Info ---

	BuildInfo = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "scylla2ch_build_info",
			Help: "Build information (version, go_version)",
		},
		[]string{"version", "go_version"},
	)
)

func init() {
	prometheus.MustRegister(
		RowsConsumed,
		RowsFlushed,
		RowsDropped,
		RowsDLQ,
		FlushesTotal,
		FlushLatency,
		FlushBatchSize,
		BufferSize,
		DeletesSkipped,
		ConsumersActive,
		CDCLagSeconds,
		RetriesTotal,
		TablesActive,
		HotReloadTotal,
		SighupReloadTotal,
		ErrorsTotal,
		ReaderErrorsTotal,
		ScyllaConnected,
		ClickHouseConnected,
		UptimeSeconds,
		LastConsumedTimestampMs,
		LastFlushedTimestampMs,
		GoroutineCount,
		HeapAllocBytes,
		BuildInfo,
		CDCGapSeconds,
		DLQBytes,
		CheckpointErrorsTotal,
	)

}

// SetBuildInfo sets the build info metric with the given version.
// Call from main/cmd after ldflags injection.
func SetBuildInfo(version string) {
	BuildInfo.WithLabelValues(version, runtime.Version()).Set(1)
}

// NewServer creates an HTTP server that serves Prometheus metrics.
func NewServer(cfg config.MetricsConfig) *http.Server {
	mux := http.NewServeMux()
	mux.Handle(cfg.Path, promhttp.Handler())
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`<html><body><h1>scylla2ch</h1>
<p>Direct ScyllaDB CDC → ClickHouse replicator</p>
<p><a href="` + cfg.Path + `">Metrics</a></p>
</body></html>`))
	})

	return &http.Server{
		Addr:              fmt.Sprintf(":%d", cfg.Port),
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
}
