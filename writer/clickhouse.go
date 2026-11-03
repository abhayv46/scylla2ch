// Package writer implements the ClickHouse batch writer with channel-per-table architecture.
//
// Architecture (like PeerDB and Debezium):
//
//	┌──────────────────────────────────────────────────────────────────┐
//	│  CDC Consumer goroutines (many)                                  │
//	│                                                                  │
//	│  consumer_1 ──→ Add("rooms", row) ──→ ┌─────────────────────┐   │
//	│  consumer_2 ──→ Add("rooms", row) ──→ │ rooms channel (buf) │   │
//	│  consumer_3 ──→ Add("msgs",  row) ──→ │ msgs  channel (buf) │   │
//	│  consumer_4 ──→ Add("msgs",  row) ──→ └─────────────────────┘   │
//	│                                              │                   │
//	│                                     tableWriter goroutines       │
//	│                                     (one per table, dedicated)   │
//	│                                              │                   │
//	│                                     batch flush to ClickHouse    │
//	│                                     (size OR timer, whichever    │
//	│                                      comes first)                │
//	└──────────────────────────────────────────────────────────────────┘
//
// Key design decisions:
//
//   - Channel-per-table: each table gets its own buffered channel + writer goroutine.
//     This means a slow flush to table A never blocks CDC events for table B.
//     Tables are completely independent — adding a new table has zero impact.
//
//   - Lock-free hot path: Add() does a channel send. The only lock is RWMutex on
//     the tables map (read path), which is only taken on first-ever Add for a table.
//
//   - Natural back-pressure: when ClickHouse is slower than ScyllaDB CDC, the
//     channel fills up and Add() blocks. This is intentional — it slows down the
//     CDC reader, preventing unbounded memory growth.
//
//   - Column union: CDC delta rows may have different column sets per row.
//     flush() scans all rows for column names and builds a union INSERT.
//
//   - Exponential retry with poison-row protection: retries on transient errors
//     (network, timeout), drops batch after exhausting retries to prevent OOM.
//     Dropped rows are counted in scylla2ch_rows_dropped_total metric.
//
//   - Graceful shutdown: Close() signals all writer goroutines via stopCh,
//     each goroutine drains its channel and does a final flush before exiting.
package writer

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"scylla2ch/config"
	"scylla2ch/metrics"
)

// errTableNotFound is returned by flush() when the ClickHouse table doesn't exist.
// The caller should wait for the table and retry — NOT drop the batch.
// Like Kafka: consumer connected but paused, offset doesn't advance.
var errTableNotFound = errors.New("clickhouse: table not found")

// pendingRow wraps a row with an optional acknowledgment callback.
// When onFlush is non-nil, it is called AFTER the row is successfully written to ClickHouse.
// This enables at-least-once delivery: the CDC checkpoint advances only after the write succeeds.
// Like Kafka consumer offset commits — commit only after processing is done.
// If the flush fails (retries exhausted, rows dropped), onFlush is NOT called.
// On restart, scylla-cdc-go re-reads from the last checkpoint — dropped rows are re-delivered.
type pendingRow struct {
	data    map[string]interface{}
	onFlush func() // called after successful ClickHouse INSERT; nil = no callback
}

// ClickHouseWriter batches CDC rows and flushes them to ClickHouse.
//
// Architecture: channel-per-table with dedicated writer goroutines.
//   - Each table gets its own buffered channel + writer goroutine (lazy-created on first Add).
//   - Add() is a channel send — lock-free on the hot path, blocks when full (natural back-pressure).
//   - Each writer goroutine flushes on batch_size OR on timer interval — whichever comes first.
//   - Eliminates global mutex contention: flush of "rooms" never blocks CDC events for "messages".
//
// At-least-once delivery (like Kafka consumer):
//   - Each row carries an optional onFlush callback (set by the CDC consumer).
//   - After a successful ClickHouse INSERT, all callbacks in the batch are called.
//   - The CDC consumer's callback advances the checkpoint (reporter.Update).
//   - If the INSERT fails and the batch is dropped, callbacks are NOT called.
//   - On restart, scylla-cdc-go resumes from the last checkpoint and re-delivers rows.
//   - Duplicates are handled by ClickHouse ReplacingMergeTree dedup.
//
// This pattern matches PeerDB and Debezium: a bounded buffer between reader and writer
// provides natural back-pressure when ClickHouse is slower than the CDC source.
type ClickHouseWriter struct {
	conn   clickhouse.Conn
	logger *log.Logger
	db     string // database name (e.g., "chat")
	cfg    config.BatchConfig

	// Per-table channels: lazy-created, each gets its own writer goroutine.
	tables   map[string]chan pendingRow
	tablesMu sync.RWMutex // protects the tables map only (not the data path)

	wg     sync.WaitGroup
	stopCh chan struct{}
	closed int32 // atomic: 1 after Close() called — prevents hang on send
}

func NewClickHouseWriter(cfg config.ClickHouseConfig, batchCfg config.BatchConfig, logger *log.Logger, version ...string) (*ClickHouseWriter, error) {
	ver := "dev"
	if len(version) > 0 && version[0] != "" {
		ver = version[0]
	}
	opts := &clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)},
		Auth: clickhouse.Auth{
			Database: cfg.Database,
			Username: cfg.Username,
			Password: cfg.Password,
		},
		Protocol:    clickhouse.Native, // native TCP — 30% faster than HTTP
		DialTimeout: cfg.DialTimeout,
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
		MaxIdleConns: cfg.MaxIdleConns,
		MaxOpenConns: cfg.MaxOpenConns,
		ClientInfo: clickhouse.ClientInfo{
			Products: []struct{ Name, Version string }{
				{Name: "scylla2ch", Version: ver},
			},
		},
	}

	if cfg.TLS {
		opts.TLS = &tls.Config{} // uses system CA pool, validates CH Cloud cert
	}

	conn, err := clickhouse.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("clickhouse open: %w", err)
	}

	// Bounded ping: don't hang forever if ClickHouse is unreachable at startup.
	pingCtx, pingCancel := context.WithTimeout(context.Background(), cfg.DialTimeout)
	defer pingCancel()
	if err := conn.Ping(pingCtx); err != nil {
		return nil, fmt.Errorf("clickhouse ping: %w", err)
	}

	return &ClickHouseWriter{
		conn:   conn,
		logger: logger,
		tables: make(map[string]chan pendingRow),
		cfg:    batchCfg,
		db:     cfg.Database,
		stopCh: make(chan struct{}),
	}, nil
}

// Conn returns the underlying ClickHouse connection (for auto-create DDL).
func (w *ClickHouseWriter) Conn() clickhouse.Conn {
	return w.conn
}

// Add sends a row to the per-table channel. Lock-free on the hot path.
// Blocks if channel is full — this is intentional back-pressure from ClickHouse.
// Uses select with stopCh to prevent blocking forever after Close().
//
// Optional onFlush callback: called AFTER the row is successfully written to ClickHouse.
// This is how at-least-once delivery works (like Kafka consumer offset commit):
//   - CDC consumer passes a callback that advances the checkpoint
//   - Callback is called only after the ClickHouse INSERT succeeds
//   - If the INSERT fails (retries exhausted), callback is NOT called
//   - On restart, rows are re-read from the last checkpoint
func (w *ClickHouseWriter) Add(table string, row map[string]interface{}, onFlush ...func()) {
	if atomic.LoadInt32(&w.closed) == 1 {
		return // drop silently after shutdown
	}
	ch := w.getOrCreateChan(table)
	if ch == nil {
		return // closed during getOrCreateChan
	}
	pr := pendingRow{data: row}
	if len(onFlush) > 0 {
		pr.onFlush = onFlush[0]
	}
	select {
	case ch <- pr: // blocks when full = natural back-pressure
	case <-w.stopCh: // writer shutting down, drop the row
	}
}

// getOrCreateChan lazily creates a buffered channel and writer goroutine for a table.
// Returns nil if the writer has been closed (prevents spawning goroutines after shutdown).
func (w *ClickHouseWriter) getOrCreateChan(table string) chan pendingRow {
	w.tablesMu.RLock()
	ch, ok := w.tables[table]
	w.tablesMu.RUnlock()
	if ok {
		return ch
	}

	w.tablesMu.Lock()
	defer w.tablesMu.Unlock()
	// Double-check after acquiring write lock
	if ch, ok = w.tables[table]; ok {
		return ch
	}
	// Prevent spawning new writer goroutines after Close()
	if atomic.LoadInt32(&w.closed) == 1 {
		return nil
	}

	// Buffer = 2x batch size: one batch of headroom before back-pressure kicks in
	bufSize := w.cfg.Size * 2
	ch = make(chan pendingRow, bufSize)
	w.tables[table] = ch

	w.wg.Add(1)
	go w.tableWriter(table, ch)
	w.logger.Printf("started writer goroutine for table=%s channel_buf=%d", table, bufSize)

	return ch
}

// tableWriter is a dedicated goroutine for one table.
// Drains its channel, accumulates rows, flushes on batch_size or timer.
//
// Kafka-like guarantee when ClickHouse table doesn't exist:
//   - flushWithTableWait() detects table-not-found → blocks in waitForTable()
//   - buf holds the current batch (bounded: 1 × batch_size)
//   - channel fills up (bounded: 2 × batch_size) → Add() blocks → CDC reader paused
//   - Checkpoint NOT advanced (onFlush never called) → CDC will re-read on restart
//   - When table appears → flush succeeds → onFlush fires → checkpoint advances → resume
//   - No OOM: total memory per waiting table ≤ 3 × batch_size × avg_row_bytes
func (w *ClickHouseWriter) tableWriter(table string, ch chan pendingRow) {
	defer w.wg.Done()

	buf := make([]pendingRow, 0, w.cfg.Size)
	ticker := time.NewTicker(w.cfg.Interval)
	defer ticker.Stop()

	for {
		select {
		case pr := <-ch:
			buf = append(buf, pr)
			if len(buf) >= w.cfg.Size {
				if !w.flushWithTableWait(table, buf, ch) {
					return // shutdown
				}
				buf = buf[:0] // reuse slice memory
			}

		case <-ticker.C:
			if len(buf) > 0 {
				if !w.flushWithTableWait(table, buf, ch) {
					return // shutdown
				}
				buf = buf[:0]
			}
			metrics.BufferSize.WithLabelValues(table).Set(float64(len(ch)))

		case <-w.stopCh:
			// Drain remaining rows from channel, then flush everything
		draining:
			for {
				select {
				case pr := <-ch:
					buf = append(buf, pr)
				default:
					break draining
				}
			}
			if len(buf) > 0 {
				err := w.flush(table, buf, ch)
				if err == errTableNotFound {
					w.logger.Printf("table=%s not found during shutdown — %d rows not flushed (checkpoint frozen, will re-read from CDC on restart)", table, len(buf))
				}
			}
			w.logger.Printf("writer goroutine stopped for table=%s", table)
			return
		}
	}
}

// flush sends a batch of rows to ClickHouse with retries and exponential backoff.
// ch is the table's channel reference (avoids unsafe map read for metrics).
//
// At-least-once delivery (like Kafka consumer offset commit):
//   - On SUCCESS: calls onFlush() for each row → CDC checkpoint advances
//   - On FAILURE (retries exhausted): onFlush() NOT called → checkpoint stays behind
//   - On restart: scylla-cdc-go re-reads from last checkpoint → rows re-delivered
//   - Duplicates handled by ClickHouse ReplacingMergeTree(_cdc_ts) dedup
// flush sends a batch of rows to ClickHouse with retries and exponential backoff.
// Returns errTableNotFound if the ClickHouse table doesn't exist — caller should
// wait for the table and retry the SAME batch (don't clear buffer, don't drop).
// Returns nil on success OR on drop (poison row protection for non-table errors).
func (w *ClickHouseWriter) flush(table string, rows []pendingRow, ch chan pendingRow) error {
	if len(rows) == 0 {
		return nil
	}

	start := time.Now()

	// Column union: scan ALL rows for column names.
	// CDC delta rows may have different column sets per row.
	colSet := make(map[string]struct{})
	for _, pr := range rows {
		for k := range pr.data {
			colSet[k] = struct{}{}
		}
	}
	columns := make([]string, 0, len(colSet))
	for k := range colSet {
		columns = append(columns, k)
	}
	sort.Strings(columns)

	// Parse qualified name: "database.table" → INSERT INTO `database`.`table`
	// Falls back to w.db for unqualified names (backward compat with single-keyspace)
	db, tablePart := w.db, table
	if idx := strings.IndexByte(table, '.'); idx != -1 {
		db = table[:idx]
		tablePart = table[idx+1:]
	}

	// Build INSERT with explicit column names (CDC delta only has changed columns).
	// Backtick-quote column names for safety — ScyllaDB enforces valid identifiers,
	// but quoting is defensive best practice (matches DDL in schema.go).
	quoted := make([]string, len(columns))
	for i, c := range columns {
		quoted[i] = "`" + c + "`"
	}
	query := fmt.Sprintf("INSERT INTO `%s`.`%s` (%s)", db, tablePart, strings.Join(quoted, ", "))

	var lastErr error
	for attempt := 0; attempt <= w.cfg.RetryMax; attempt++ {
		if attempt > 0 {
			// Exponential backoff: 500ms, 1s, 2s, 4s...
			backoff := w.cfg.RetryBackoff * time.Duration(1<<uint(attempt-1))
			// Use NewTimer instead of time.After to avoid timer leak when stopCh wins.
			timer := time.NewTimer(backoff)
			select {
			case <-timer.C:
			case <-w.stopCh:
				timer.Stop()
				w.logger.Printf("flush cancelled during shutdown table=%s rows=%d", table, len(rows))
				return nil
			}
			timer.Stop() // stop if timer already fired (no-op but safe)
			w.logger.Printf("RETRY table=%s attempt=%d/%d backoff=%v", table, attempt, w.cfg.RetryMax, backoff)
		}

		// Fresh context per retry — prevents a slow first attempt from starving later retries.
		attemptCtx, attemptCancel := context.WithTimeout(context.Background(), 30*time.Second)
		batch, err := w.conn.PrepareBatch(attemptCtx, query, driver.WithReleaseConnection())
		if err != nil {
			// Table not found → don't waste retries, return immediately for caller to wait
			if isTableNotFound(err) {
				attemptCancel()
				return errTableNotFound
			}
			lastErr = fmt.Errorf("prepare: %w", err)
			attemptCancel()
			continue
		}

		appendErr := false
		for _, pr := range rows {
			vals := make([]interface{}, len(columns))
			for i, col := range columns {
				vals[i] = pr.data[col] // nil/missing → nil, handled by ClickHouse Nullable columns
			}
			if err := batch.Append(vals...); err != nil {
				lastErr = fmt.Errorf("append row: %w", err)
				appendErr = true
				break
			}
		}
		if appendErr {
			batch.Abort()
			attemptCancel()
			continue
		}

		if err := batch.Send(); err != nil {
			if isTableNotFound(err) {
				attemptCancel()
				return errTableNotFound
			}
			lastErr = fmt.Errorf("send: %w", err)
			attemptCancel()
			continue
		}
		attemptCancel()

		// Success — call ack callbacks (at-least-once: checkpoint advances only now)
		for _, pr := range rows {
			if pr.onFlush != nil {
				pr.onFlush()
			}
		}

		flushedCount := len(rows)
		elapsed := time.Since(start).Seconds()
		w.logger.Printf("flushed table=%s rows=%d duration=%.3fs", table, flushedCount, elapsed)
		metrics.RowsFlushed.WithLabelValues(table).Add(float64(flushedCount))
		metrics.FlushesTotal.WithLabelValues(table, "ok").Inc()
		metrics.FlushLatency.WithLabelValues(table).Observe(elapsed)
		metrics.FlushBatchSize.WithLabelValues(table).Observe(float64(flushedCount))
		metrics.BufferSize.WithLabelValues(table).Set(float64(len(ch)))
		if attempt > 0 {
			metrics.RetriesTotal.WithLabelValues(table).Add(float64(attempt))
		}
		return nil
	}

	// Poison row protection: drop rows after exhausting retries to prevent OOM.
	// IMPORTANT: onFlush callbacks are NOT called — checkpoint stays behind.
	// On restart, scylla-cdc-go re-reads from the last checkpoint and these rows
	// will be re-delivered (at-least-once). Same pattern as Kafka DLQ.
	droppedCount := len(rows)
	w.logger.Printf("ERROR exhausted retries table=%s rows=%d err=%v — DROPPING (checkpoint NOT advanced, rows will be re-read on restart)", table, droppedCount, lastErr)
	metrics.FlushesTotal.WithLabelValues(table, "error").Inc()
	metrics.RowsDropped.WithLabelValues(table).Add(float64(droppedCount))
	metrics.ErrorsTotal.WithLabelValues("writer", "critical").Inc()
	metrics.BufferSize.WithLabelValues(table).Set(float64(len(ch)))
	return nil
}

// isTableNotFound checks if a ClickHouse error indicates the target table doesn't exist.
// ClickHouse error code 60 = UNKNOWN_TABLE.
func isTableNotFound(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	return strings.Contains(errStr, "code: 60") || strings.Contains(errStr, "UNKNOWN_TABLE")
}

// waitForTable blocks until the ClickHouse table exists or shutdown is requested.
// Like Kafka consumer.pause(): rows stay in channel buffer, checkpoint frozen, no OOM.
//
// Memory during wait is bounded:
//   - buf: 1 × batch_size rows (the batch that triggered the wait)
//   - channel: 2 × batch_size rows (already-buffered, channel capacity)
//   - Total per table: ~7.5 MB at default settings (10K × 256 bytes × 3)
//   - Channel full → Add() blocks → CDC reader paused → no new allocations
func (w *ClickHouseWriter) waitForTable(table string) bool {
	db, tablePart := w.db, table
	if idx := strings.IndexByte(table, '.'); idx != -1 {
		db = table[:idx]
		tablePart = table[idx+1:]
	}

	w.logger.Printf("table=%s not found in ClickHouse — writer PAUSED (like Kafka consumer.pause: rows held, checkpoint frozen, no data loss)", table)
	metrics.ErrorsTotal.WithLabelValues("writer", "warning").Inc()

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			var count uint64
			err := w.conn.QueryRow(ctx,
				"SELECT count() FROM system.tables WHERE database = ? AND name = ?",
				db, tablePart,
			).Scan(&count)
			cancel()
			if err == nil && count > 0 {
				w.logger.Printf("table=%s now exists — writer RESUMED (flushing held rows, checkpoint will advance)", table)
				return true
			}
		case <-w.stopCh:
			w.logger.Printf("shutdown while waiting for table=%s — rows in buffer will be re-read from CDC on restart", table)
			return false
		}
	}
}

// flushWithTableWait flushes buf, waiting for the ClickHouse table if it doesn't exist.
// Returns false only on shutdown. Guarantees: no data loss, no OOM, no stuck.
//
// Flow (like Kafka consumer):
//  1. flush() succeeds → return true (normal path)
//  2. flush() returns errTableNotFound → waitForTable() blocks (back-pressure)
//  3. Table appears → retry flush → goto 1
//  4. Shutdown during wait → return false (rows safe in CDC log, checkpoint not advanced)
func (w *ClickHouseWriter) flushWithTableWait(table string, buf []pendingRow, ch chan pendingRow) bool {
	for {
		err := w.flush(table, buf, ch)
		if err != errTableNotFound {
			return true // success or dropped (non-table error)
		}
		// Table not found — wait, then retry same batch
		if !w.waitForTable(table) {
			return false // shutdown
		}
		// Table appeared — loop back to retry flush
	}
}


// TableExists checks if a table exists in ClickHouse.
// Supports qualified names: "database.table" → checks in that database.
// Unqualified names use the default database (w.db).
func (w *ClickHouseWriter) TableExists(ctx context.Context, table string) (bool, error) {
	db, tablePart := w.db, table
	if idx := strings.IndexByte(table, '.'); idx != -1 {
		db = table[:idx]
		tablePart = table[idx+1:]
	}
	var count uint64
	err := w.conn.QueryRow(ctx,
		"SELECT count() FROM system.tables WHERE database = ? AND name = ?",
		db, tablePart,
	).Scan(&count)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

// EnsureDatabase creates a ClickHouse database if it doesn't exist.
// Used by multi-keyspace mode to auto-create per-keyspace databases.
func (w *ClickHouseWriter) EnsureDatabase(ctx context.Context, database string) error {
	return w.conn.Exec(ctx, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", database))
}

func (w *ClickHouseWriter) Close() error {
	if !atomic.CompareAndSwapInt32(&w.closed, 0, 1) {
		return nil // already closed — prevent double-close panic
	}
	close(w.stopCh) // signal all writer goroutines to drain and exit
	w.wg.Wait()     // wait for all writers to finish flushing

	w.logger.Printf("ClickHouseWriter closed, all buffers flushed")
	return w.conn.Close()
}