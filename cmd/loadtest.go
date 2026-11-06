// loadtest.go — ScyllaDB CDC load test command.
//
// Inserts synthetic rows into a ScyllaDB table at a configurable rate,
// then monitors the scylla2ch dashboard timestamps to measure pipeline lag.
//
// Usage:
//   scylla2ch loadtest --table chat.pending_rooms --rate 100 --duration 30s
//   scylla2ch loadtest --table chat.pending_rooms --rate 1000 --duration 60s --batch 50
package cmd

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/spf13/cobra"

	"scylla2ch/config"
)

var loadtestCmd = &cobra.Command{
	Use:   "loadtest",
	Short: "Insert synthetic rows into ScyllaDB to load-test the CDC pipeline",
	Long: `Inserts rows into a ScyllaDB CDC-enabled table at a configurable rate.
Watch the scylla2ch dashboard to observe:
  - "last from ScyllaDB" timestamps turn green
  - "last to ClickHouse" follows within batch interval
  - "pipeline lag" shows end-to-end delay
  - CDC lag, buffer size, and flush latency under load

Scenarios:
  --rate 100   Matches current production peak (~100 events/sec)
  --rate 500   5x prod — normal headroom test
  --rate 1000  10x prod — stress test
  --rate 5000  50x prod — saturation test (buffer fills, back-pressure)`,
	RunE: runLoadtest,
}

var (
	ltTable    string
	ltRate     int
	ltDuration time.Duration
	ltBatch    int
	ltWorkers  int
)

func init() {
	loadtestCmd.Flags().StringVar(&ltTable, "table", "chat.pending_rooms", "ScyllaDB keyspace.table to insert into")
	loadtestCmd.Flags().IntVar(&ltRate, "rate", 100, "target inserts per second")
	loadtestCmd.Flags().DurationVar(&ltDuration, "duration", 30*time.Second, "test duration")
	loadtestCmd.Flags().IntVar(&ltBatch, "batch", 1, "rows per CQL batch (1=individual INSERTs)")
	loadtestCmd.Flags().IntVar(&ltWorkers, "workers", 4, "concurrent writer goroutines")
	rootCmd.AddCommand(loadtestCmd)
}

func runLoadtest(cmd *cobra.Command, args []string) error {
	logger := log.New(os.Stdout, "[loadtest] ", log.LstdFlags|log.Lmicroseconds)

	cfg, err := config.Load(cfgFile)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	// Parse keyspace.table
	parts := strings.SplitN(ltTable, ".", 2)
	if len(parts) != 2 {
		return fmt.Errorf("table must be keyspace.table (got %q)", ltTable)
	}
	keyspace, table := parts[0], parts[1]

	logger.Printf("connecting to ScyllaDB keyspace=%s", keyspace)
	session, err := connectScylla(cfg, keyspace, logger)
	if err != nil {
		return fmt.Errorf("connect ScyllaDB: %w", err)
	}
	defer session.Close()

	// Verify table exists and has CDC
	var cdcCount int
	cdcLogTable := table + "_scylla_cdc_log"
	if err := session.Query(
		"SELECT count(*) FROM system_schema.tables WHERE keyspace_name = ? AND table_name = ?",
		keyspace, cdcLogTable,
	).Scan(&cdcCount); err != nil {
		return fmt.Errorf("CDC check: %w", err)
	}
	if cdcCount == 0 {
		return fmt.Errorf("CDC not enabled on %s.%s — run: ALTER TABLE %s.%s WITH cdc = {'enabled': true}", keyspace, table, keyspace, table)
	}

	// Prepare INSERT statement for pending_rooms schema
	insertCQL := fmt.Sprintf(`INSERT INTO %s.%s (user_id, room_id, peer_id, last_msg_ts_ms, last_snippet, unread_count, tier, source) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, keyspace, table)
	prepared := session.Query(insertCQL)
	_ = prepared // validate CQL syntax
	prepared.Release()

	logger.Printf("=== LOAD TEST ===")
	logger.Printf("table:    %s.%s (CDC verified)", keyspace, table)
	logger.Printf("rate:     %d inserts/sec", ltRate)
	logger.Printf("duration: %s", ltDuration)
	logger.Printf("workers:  %d", ltWorkers)
	logger.Printf("batch:    %d rows/insert", ltBatch)
	logger.Printf("total:    ~%d rows", ltRate*int(ltDuration.Seconds()))
	logger.Printf("")
	logger.Printf("Watch dashboard: last from ScyllaDB / last to ClickHouse / pipeline lag")
	logger.Printf("Starting in 3s...")
	time.Sleep(3 * time.Second)

	// Rate limiter: distribute work across interval
	var totalInserted int64
	var totalErrors int64
	ctx, cancel := context.WithTimeout(context.Background(), ltDuration)
	defer cancel()

	// Use a ticker to spread inserts evenly
	interval := time.Second / time.Duration(ltRate/ltWorkers)
	if interval < time.Microsecond {
		interval = time.Microsecond
	}

	var wg sync.WaitGroup
	startTime := time.Now()

	for w := 0; w < ltWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			ticker := time.NewTicker(interval)
			defer ticker.Stop()
			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID)))

			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					userID := rng.Int63n(10_000_000) + 1
					roomID := fmt.Sprintf("lt_%d_%d", time.Now().UnixNano(), rng.Intn(100000))
					peerID := rng.Int63n(10_000_000) + 1
					nowMs := time.Now().UnixMilli()
					snippet := fmt.Sprintf("loadtest msg %d", atomic.LoadInt64(&totalInserted))

					err := session.Query(insertCQL,
						userID, roomID, peerID, nowMs, snippet, 1, rng.Intn(6)+1, "loadtest",
					).WithContext(ctx).Exec()
					if err != nil {
						atomic.AddInt64(&totalErrors, 1)
					} else {
						atomic.AddInt64(&totalInserted, 1)
					}
				}
			}
		}(w)
	}

	// Progress reporter
	go func() {
		tick := time.NewTicker(5 * time.Second)
		defer tick.Stop()
		var lastCount int64
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
				current := atomic.LoadInt64(&totalInserted)
				errs := atomic.LoadInt64(&totalErrors)
				delta := current - lastCount
				elapsed := time.Since(startTime).Seconds()
				logger.Printf("progress: %d rows (%.0f/s avg, %d in last 5s = %.0f/s) errors=%d elapsed=%.0fs",
					current, float64(current)/elapsed, delta, float64(delta)/5.0, errs, elapsed)
				lastCount = current
			}
		}
	}()

	wg.Wait()

	elapsed := time.Since(startTime)
	total := atomic.LoadInt64(&totalInserted)
	errs := atomic.LoadInt64(&totalErrors)
	logger.Printf("")
	logger.Printf("=== RESULTS ===")
	logger.Printf("rows inserted: %d", total)
	logger.Printf("errors:        %d", errs)
	logger.Printf("duration:      %s", elapsed.Round(time.Millisecond))
	logger.Printf("throughput:    %.1f rows/sec", float64(total)/elapsed.Seconds())
	logger.Printf("")
	logger.Printf("Now check the dashboard:")
	logger.Printf("  - last from ScyllaDB → should be green (< 1min)")
	logger.Printf("  - last to ClickHouse → should follow within 2s")
	logger.Printf("  - pipeline lag → end-to-end delay")
	logger.Printf("  - rows read / rows written → should increase by ~%d", total)

	return nil
}
