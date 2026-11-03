// snapshot.go — Initial bulk load command.
//
// Reads ALL rows from a ScyllaDB production table and writes them to ClickHouse.
// This is the ONLY command that reads production tables (not CDC log tables).
//
// Use case: populate ClickHouse with existing data BEFORE starting CDC streaming.
// After snapshot completes, start `scylla2ch run` — CDC picks up from where
// snapshot left off (CDC events have newer _cdc_ts, ReplacingMergeTree dedup wins).
//
// Each row gets:
//   - _cdc_operation = 0 (snapshot, not a CDC delta)
//   - _cdc_ts = now()   (baseline timestamp for ReplacingMergeTree versioning)
//
// WARNING: Only run snapshot BEFORE CDC starts. Re-running after CDC is active
// will overwrite newer CDC data (snapshot _cdc_ts=now > older CDC _cdc_ts).
//
// Supports:
//   - Multi-target routing (same as CDC consumer)
//   - Column skipping (skip_columns config)
//   - Progress logging (every 100K rows)
//   - Same ConvertValue() type mapping as CDC consumer
package cmd

import (
	"fmt"
	"log"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"scylla2ch/config"
	"scylla2ch/consumer"
	"scylla2ch/writer"
)

var (
	snapshotTable    string
	snapshotKeyspace string
)

func init() {
	rootCmd.AddCommand(snapshotCmd)
	snapshotCmd.Flags().StringVar(&snapshotTable, "table", "", "table name to snapshot (e.g., rooms)")
	snapshotCmd.Flags().StringVar(&snapshotKeyspace, "keyspace", "", "keyspace to snapshot from (defaults to first configured keyspace)")
	snapshotCmd.MarkFlagRequired("table")
}

var snapshotCmd = &cobra.Command{
	Use:   "snapshot",
	Short: "Initial bulk load: read full ScyllaDB table and write to ClickHouse",
	Long: `Reads all rows from a ScyllaDB table and inserts them into the corresponding
ClickHouse table. Use this for initial data load before starting CDC streaming.

NOTE: This reads from the ORIGINAL table (not CDC log) because CDC log tables
only have recent data (8h TTL). This is the ONLY command that reads production
tables — the 'run' command reads ONLY from CDC log tables.

WARNING: Snapshot sets _cdc_ts=now64() — only use for initial load BEFORE CDC starts.
Re-running snapshot after CDC is active will overwrite newer CDC data.`,
	RunE: runSnapshot,
}

func runSnapshot(cmd *cobra.Command, args []string) error {
	cfg, err := config.Load(cfgFile)
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}

	logger := log.New(os.Stderr, "[snapshot] ", log.Ldate|log.Ltime|log.Lmicroseconds)

	// Snapshot uses the --keyspace flag if provided, otherwise first configured keyspace
	keyspaces := cfg.Scylla.AllKeyspaces()
	if len(keyspaces) == 0 {
		return fmt.Errorf("no keyspace configured")
	}
	keyspace := keyspaces[0]
	if snapshotKeyspace != "" {
		found := false
		for _, ks := range keyspaces {
			if ks == snapshotKeyspace {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("keyspace %q not in configured keyspaces %v", snapshotKeyspace, keyspaces)
		}
		keyspace = snapshotKeyspace
	}

	// Connect to ScyllaDB
	session, err := connectScylla(cfg, keyspace, logger)
	if err != nil {
		return err
	}
	defer session.Close()

	// Validate table name (prevent CQL injection from misconfigured input)
	if !regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9_]*$`).MatchString(snapshotTable) {
		return fmt.Errorf("invalid table name: %q", snapshotTable)
	}

	// Connect to ClickHouse
	chWriter, err := writer.NewClickHouseWriter(cfg.ClickHouse, cfg.Batch, logger, Version)
	if err != nil {
		return fmt.Errorf("clickhouse connect: %w", err)
	}
	defer chWriter.Close()

	// Find table config
	tc := cfg.GetTableConfig(snapshotTable)
	skipCols := make(map[string]bool)
	if tc != nil {
		for _, s := range tc.SkipColumns {
			skipCols[s] = true
		}
	}

	// Read from ScyllaDB (this IS the original table — only time we read it)
	fullTable := fmt.Sprintf("%s.%s", keyspace, snapshotTable)
	logger.Printf("starting snapshot of %s (reads ORIGINAL table for initial load)", fullTable)

	query := fmt.Sprintf(`SELECT * FROM "%s"."%s"`, keyspace, snapshotTable)
	iter := session.Query(query).
		PageSize(5000).
		Iter()

	columns := iter.Columns()
	if len(columns) == 0 {
		return fmt.Errorf("table %s not found or empty", fullTable)
	}

	// Build column indexes (skip CDC internal columns and user-skipped columns)
	var colIndexes []int
	for i, col := range columns {
		if skipCols[col.Name] {
			continue
		}
		if strings.HasPrefix(col.Name, "cdc$") {
			continue
		}
		colIndexes = append(colIndexes, i)
	}

	// Determine ClickHouse database for this keyspace (constant for all rows).
	// Single-keyspace: use clickhouse.database from config (backward compat).
	// Multi-keyspace: use keyspace name as database name.
	chDB := keyspace
	if len(keyspaces) == 1 && cfg.ClickHouse.Database != "" {
		chDB = cfg.ClickHouse.Database
	}
	chTarget := fmt.Sprintf("%s.%s", chDB, snapshotTable)

	totalRows := 0
	skippedRows := 0
	startTime := time.Now()
	snapshotTime := startTime // consistent _cdc_ts for all snapshot rows

	// Scan all rows — pre-allocate slices once and reuse across rows
	scanner := iter.Scanner()
	values := make([]interface{}, len(columns))
	ptrs := make([]interface{}, len(columns))
	for i := range values {
		ptrs[i] = &values[i]
	}
	for scanner.Next() {
		// Reset values for reuse (clear previous row data)
		for i := range values {
			values[i] = nil
		}

		if err := scanner.Scan(ptrs...); err != nil {
			logger.Printf("ERROR scan: %v", err)
			skippedRows++
			continue
		}

		row := make(map[string]interface{}, len(colIndexes)+2) // +2 for _cdc_operation, _cdc_ts
		for _, idx := range colIndexes {
			val := values[idx]
			if val == nil {
				continue
			}
			name := columns[idx].Name

			// Use the same type conversion as CDC consumer
			converted := consumer.ConvertValue(name, val)
			if converted != nil {
				row[name] = converted
			}
		}

		// Add CDC metadata — matches what the CDC consumer adds.
		// _cdc_ts = now so ReplacingMergeTree treats these as the baseline.
		// CDC events arriving later will have newer _cdc_ts and win during dedup.
		row["_cdc_operation"] = int8(0) // 0 = snapshot (not a CDC delta)
		row["_cdc_ts"] = snapshotTime   // consistent timestamp for entire snapshot

		// Route or direct add (with fallback to default table like CDC consumer)
		if tc != nil && len(tc.Routes) > 0 {
			matched := false
			for _, route := range tc.Routes {
				if route.Matches(row) {
					filtered := make(map[string]interface{})
					for _, col := range route.Columns {
						if v, ok := row[col]; ok {
							filtered[col] = v
						}
					}
					chWriter.Add(chDB+"."+route.Target, filtered)
					matched = true
					break
				}
			}
			if !matched {
				chWriter.Add(chTarget, row)
			}
		} else {
			chWriter.Add(chTarget, row)
		}

		totalRows++
		if totalRows%100000 == 0 {
			elapsed := time.Since(startTime)
			rate := float64(totalRows) / elapsed.Seconds()
			logger.Printf("progress: %d rows (%.0f rows/sec)", totalRows, rate)
		}
	}

	if err := scanner.Err(); err != nil {
		logger.Printf("ERROR iterator: %v", err)
		return fmt.Errorf("snapshot scan incomplete (%d rows read, %d skipped): %w", totalRows, skippedRows, err)
	}

	if skippedRows > 0 {
		logger.Printf("WARNING: skipped %d rows due to scan errors", skippedRows)
	}
	if totalRows == 0 {
		return fmt.Errorf("snapshot produced 0 rows — table empty or all rows failed scan")
	}

	elapsed := time.Since(startTime)
	rate := float64(totalRows) / elapsed.Seconds()
	logger.Printf("snapshot complete: %d rows in %v (%.0f rows/sec)", totalRows, elapsed.Round(time.Second), rate)

	return nil
}
