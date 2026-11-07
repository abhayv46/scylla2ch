// consumer.go — CDC event processor and type converter.
//
// Handles the core data transformation: ScyllaDB CDC delta rows → ClickHouse rows.
//
// Key responsibilities:
//   1. Consume() — called by scylla-cdc-go for each CDC change batch
//   2. extractValues() — reads column values from CDC delta rows
//   3. ConvertValue() — converts ALL CQL types to ClickHouse-compatible Go types
//   4. routeToTargets() — splits one ScyllaDB table → N ClickHouse tables
//
// ConvertValue is the universal type mapper. It handles:
//   - All 15 CQL scalar types (text, int, bigint, boolean, timestamp, uuid, etc.)
//   - All collection types (list, set, map, tuple, UDT — both frozen and non-frozen)
//   - Reflect-based fallback for any unknown type (future-proofing)
//   - Exported so both CDC consumer and snapshot command use the same conversion
//
// Delta mode (default): only receives columns that were actually written/updated.
// This means each CDC row may have a different column set — the ClickHouseWriter
// handles this via column union (see writer/clickhouse.go flush()).
//
// Delete handling: ALL delete types are skipped (row, partition, range, TTL service).
// ClickHouse ReplacingMergeTree handles dedup via _cdc_ts version column.
// For hard deletes, use ClickHouse TTL or manual OPTIMIZE FINAL + filter.
//
// At-least-once delivery (like Kafka consumer offset commit):
// The CDC checkpoint is advanced ONLY after ClickHouse write succeeds.
// Each row carries an onFlush callback → writer calls it after successful INSERT.
// If INSERT fails (retries exhausted, batch dropped), callback is NOT called.
// On restart, scylla-cdc-go re-reads from the last checkpoint — rows re-delivered.
// ReplacingMergeTree(_cdc_ts) deduplicates any re-delivered rows.
//
// Read-only: scylla2ch reads ONLY CDC log tables (*_scylla_cdc_log) from ScyllaDB.
// The ONLY write to ScyllaDB is the checkpoint/progress table (scylla2ch_progress)
// used by scylla-cdc-go's TableBackedProgressManager to track stream positions.
package consumer

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"math/big"
	"net"
	"reflect"
	"strings"
	"time"

	"github.com/gocql/gocql"
	"gopkg.in/inf.v0"

	scyllacdc "github.com/scylladb/scylla-cdc-go"

	"scylla2ch/config"
	"scylla2ch/metrics"
	"scylla2ch/writer"
)

// Consumer handles CDC changes for ONE stream of ONE table.
// Uses Delta mode (no postimage required) — works with all production tables.
//
// KEY: scylla-cdc-go reads ONLY from *_scylla_cdc_log tables.
// ZERO reads hit your production data tables.
type Consumer struct {
	tableName string
	shortName string // cached: "chat.rooms" → "rooms" (avoids per-row string split)
	chDB      string // ClickHouse database name (for qualified writer.Add calls)
	cfg       *config.Config
	writer    *writer.ClickHouseWriter
	logger    *log.Logger
	reporter  *scyllacdc.PeriodicProgressReporter
}

func (c *Consumer) Consume(ctx context.Context, change scyllacdc.Change) error {
	// At-least-once delivery (like Kafka consumer offset commit):
	// The checkpoint is advanced via onFlush callback AFTER ClickHouse write succeeds.
	// If the write fails, the callback is not called, checkpoint stays behind,
	// and on restart scylla-cdc-go re-reads from the last checkpoint.
	changeTime := change.Time // capture for onFlush closure
	onFlush := func() { c.reporter.Update(changeTime) }

	// Delta mode — only changed columns per CDC row.
	// Works for INSERT-only tables (all columns present). For tables with UPDATEs,
	// unchanged columns become NULL after ReplacingMergeTree merge — accepted trade-off
	// (postimage mode would fix this but adds extra read per write on ScyllaDB).
	for _, row := range change.Delta {
		op := row.GetOperation()
		// Skip ALL delete types — ClickHouse ReplacingMergeTree handles dedup, not deletes.
		// Covers:
		//   3 = RowDelete          (DELETE ... WHERE pk = ? AND ck = ?)
		//   4 = PartitionDelete    (DELETE FROM ... WHERE pk = ?)
		//   5 = RangeDeleteStartInclusive  (DELETE ... WHERE ck >= ?)
		//   6 = RangeDeleteStartExclusive  (DELETE ... WHERE ck > ?)
		//   7 = RangeDeleteEndInclusive    (DELETE ... WHERE ck <= ?)
		//   8 = RangeDeleteEndExclusive    (DELETE ... WHERE ck < ?)
		//  -3 = ServiceRowDelete       (per-row TTL expiry, ScyllaDB 2026.2+)
		//  -4 = ServicePartitionDelete (per-partition TTL expiry, ScyllaDB 2026.2+)
		// Without this, range deletes and TTL expirations would be written to ClickHouse
		// as spurious data rows with only PK columns populated — data corruption.
		isDelete := op == scyllacdc.RowDelete ||
			op == scyllacdc.PartitionDelete ||
			op == scyllacdc.RangeDeleteStartInclusive ||
			op == scyllacdc.RangeDeleteStartExclusive ||
			op == scyllacdc.RangeDeleteEndInclusive ||
			op == scyllacdc.RangeDeleteEndExclusive ||
			op == scyllacdc.OperationType(-3) || // ServiceRowDelete (TTL expiry)
			op == scyllacdc.OperationType(-4) // ServicePartitionDelete (TTL expiry)
		if isDelete {
			metrics.DeletesSkipped.WithLabelValues(c.shortTableName()).Inc()
			continue
		}

		values := c.extractValues(row)
		if len(values) == 0 {
			continue
		}

		// Add CDC metadata
		cdcTime := change.Time.Time()
		values["_cdc_ts"] = cdcTime // CDC event time for ReplacingMergeTree version ordering

		// Route: does this table have multi-target routing?
		// Pass onFlush callback — checkpoint advances only after ClickHouse write succeeds
		tableConfig := c.cfg.GetTableConfig(c.shortTableName())
		if tableConfig != nil && len(tableConfig.Routes) > 0 {
			c.routeToTargets(values, tableConfig, onFlush)
		} else {
			c.writer.Add(c.chQualifiedName(), values, onFlush)
		}

		metrics.RowsConsumed.WithLabelValues(c.shortTableName()).Inc()
		metrics.LastConsumedTimestampMs.WithLabelValues(c.shortTableName()).Set(float64(time.Now().UnixMilli()))
		// Track CDC replication lag (how far behind real-time we are)
		metrics.CDCLagSeconds.WithLabelValues(c.shortTableName()).Set(time.Since(cdcTime).Seconds())
	}

	return nil
}

// extractValues reads columns from a CDC Delta row.
// Handles ALL CQL types that scylla-cdc-go v1.2.1 returns.
//
// Type mapping (CQL → scylla-cdc-go → ClickHouse):
//   text/varchar/ascii  → *string       → String
//   bigint/counter      → *int64        → Int64
//   int                 → *int          → Int32 (Go *int, not *int32!)
//   smallint            → *int16        → Int16
//   tinyint             → *int8         → Int8
//   float               → *float32      → Float32
//   double              → *float64      → Float64
//   boolean             → *bool         → UInt8 (1/0)
//   timestamp           → *time.Time    → DateTime64
//   uuid/timeuuid       → *gocql.UUID   → String (UUID hex)
//   blob                → []byte        → String (hex)
//   inet                → *net.IP       → String
//   varint              → *big.Int      → String (decimal repr)
//   decimal             → *inf.Dec      → String (decimal repr)
//   duration            → *gocql.Duration → String
//   list<T>/set<T>      → []T           → Array(T) or JSON string
//   frozen<list/set<T>> → []T           → Array(T) or JSON string
//   map<K,V>            → map[K]V       → JSON string
//   tuple<...>          → []interface{} → JSON string
//   UDT                 → map[string]interface{} → JSON string
func (c *Consumer) extractValues(row *scyllacdc.ChangeRow) map[string]interface{} {
	cols := row.Columns()
	values := make(map[string]interface{}, len(cols))

	tc := c.cfg.GetTableConfig(c.shortTableName())

	for _, colInfo := range cols {
		name := colInfo.Name

		if strings.HasPrefix(name, "cdc$") {
			continue
		}
		if tc != nil && tc.IsColumnSkipped(name) {
			continue
		}

		rawVal, ok := row.GetValue(name)
		if !ok || rawVal == nil {
			continue
		}

		val := ConvertValue(name, rawVal)
		if val != nil {
			values[name] = val
		}
	}

	return values
}

// ConvertValue dereferences pointer types and converts to ClickHouse-compatible values.
// Exported so both CDC consumer and snapshot command use the same type conversion.
// Fully dynamic: handles ALL CQL types that scylla-cdc-go can return, plus a
// reflect-based universal fallback for any unknown slice/map/struct type.
// This makes scylla2ch work with ANY ScyllaDB table — no code changes needed.
func ConvertValue(name string, rawVal interface{}) interface{} {
	switch v := rawVal.(type) {

	// === Pointer scalar types (main path for CDC) ===

	case *string:
		if v != nil {
			return *v
		}
	case *int64:
		if v != nil {
			return *v
		}
	case *int:
		// CQL int → Go *int (NOT *int32 in scylla-cdc-go)
		// Always return int32 — CQL int is 32-bit, mixed types in one batch cause poison rows.
		// Log warning on overflow (indicates corrupt CDC data, not a real CQL int).
		if v != nil {
			if *v > math.MaxInt32 || *v < math.MinInt32 {
				log.Printf("WARNING column=%s int overflow value=%d, truncating to int32 (CQL int is 32-bit)", name, *v)
			}
			return int32(*v)
		}
	case *int32:
		if v != nil {
			return *v
		}
	case *int16:
		if v != nil {
			return *v
		}
	case *int8:
		if v != nil {
			return *v
		}
	case *float32:
		if v != nil {
			return *v
		}
	case *float64:
		if v != nil {
			return *v
		}
	case *bool:
		// ClickHouse UInt8: true→1, false→0
		if v != nil {
			if *v {
				return uint8(1)
			}
			return uint8(0)
		}

	// === Time types ===

	case *time.Time:
		if v != nil {
			return v.UnixMilli()
		}
	case time.Time:
		return v.UnixMilli()
	case *gocql.UUID:
		if v != nil {
			return v.String()
		}
	case gocql.UUID:
		return v.String()

	// === Binary/network types ===

	case []byte:
		if v != nil {
			return hex.EncodeToString(v)
		}
	case *net.IP:
		if v != nil {
			return v.String()
		}
	case net.IP:
		return v.String()

	// === Arbitrary precision types ===

	case *big.Int:
		if v != nil {
			return v.String()
		}
	case *inf.Dec:
		if v != nil {
			return v.String()
		}

	// === Duration ===

	case *gocql.Duration:
		if v != nil {
			return fmt.Sprintf("%v", *v)
		}
	case gocql.Duration:
		return fmt.Sprintf("%v", v)

	// === Collection types — all element types ===

	case []string:
		return v
	case []int:
		// CQL list<int> → always int32 to avoid mixed-type batch failures.
		out := make([]int32, len(v))
		for i, x := range v {
			out[i] = int32(x)
		}
		return out
	case []int32:
		return v
	case []int64:
		return v
	case []int16:
		return v
	case []int8:
		return v
	case []float32:
		return v
	case []float64:
		return v
	case []bool:
		// list<boolean> → Array(UInt8)
		out := make([]uint8, len(v))
		for i, x := range v {
			if x {
				out[i] = 1
			}
		}
		return out
	case []gocql.UUID:
		// list<uuid>, set<uuid> → Array(String)
		out := make([]string, len(v))
		for i, x := range v {
			out[i] = x.String()
		}
		return out
	case []time.Time:
		// list<timestamp> → Array(Int64)
		out := make([]int64, len(v))
		for i, x := range v {
			out[i] = x.UnixMilli()
		}
		return out
	case []net.IP:
		// list<inet> → Array(String)
		out := make([]string, len(v))
		for i, x := range v {
			out[i] = x.String()
		}
		return out
	case [][]byte:
		// list<blob> → Array(String)
		out := make([]string, len(v))
		for i, x := range v {
			out[i] = hex.EncodeToString(x)
		}
		return out

	// === Map types → JSON string for ClickHouse ===

	case map[string]string:
		return toJSON(v)
	case map[string]interface{}:
		return toJSON(v) // UDT or map<text, ...>
	case map[string]int64:
		return toJSON(v)
	case map[string]int:
		return toJSON(v)
	case map[string]float64:
		return toJSON(v)
	case map[string]bool:
		return toJSON(v)
	case map[int64]string:
		return toJSON(v)
	case map[int]string:
		return toJSON(v)

	// === Tuple → JSON string ===

	case []interface{}:
		return toJSON(v)

	// === Direct value types (non-pointer, rare in CDC but possible) ===

	case string:
		return v
	case int64:
		return v
	case int32:
		return v
	case int:
		if v > math.MaxInt32 || v < math.MinInt32 {
			log.Printf("WARNING column=%s int overflow value=%d, truncating to int32", name, v)
		}
		return int32(v)
	case int16:
		return v
	case int8:
		return v
	case float32:
		return v
	case float64:
		return v
	case bool:
		if v {
			return uint8(1)
		}
		return uint8(0)

	default:
		// Universal fallback: use reflect to handle ANY slice, map, or struct type
		// that scylla-cdc-go might return for complex/nested CQL types.
		// This ensures scylla2ch works with any ScyllaDB table without code changes.
		rv := reflect.ValueOf(rawVal)
		if rv.Kind() == reflect.Ptr {
			if rv.IsNil() {
				return nil
			}
			return ConvertValue(name, rv.Elem().Interface())
		}
		if rv.Kind() == reflect.Slice || rv.Kind() == reflect.Map || rv.Kind() == reflect.Struct {
			return toJSON(rawVal)
		}
		return fmt.Sprintf("%v", rawVal)
	}

	return nil // nil pointer = NULL, skip
}

// routeToTargets handles 1 ScyllaDB table → N ClickHouse tables.
// Supports:
//   - 1:1 rename: route with no filter + no columns → all columns, different target name
//   - Split: route with filter + columns → subset of columns to specific target
//   - Fully-qualified target: "database.table" used as-is, bare "table" gets chDB prepended
func (c *Consumer) routeToTargets(values map[string]interface{}, tc *config.TableConfig, onFlush func()) {
	for _, route := range tc.Routes {
		if route.Matches(values) {
			// If columns not specified, pass all columns (1:1 rename / simple mapping)
			row := values
			if len(route.Columns) > 0 {
				row = make(map[string]interface{}, len(route.Columns)+2)
				for _, col := range route.Columns {
					if v, ok := values[col]; ok {
						row[col] = v
					}
				}
				// Always include CDC metadata — required for ReplacingMergeTree dedup.
				// Users won't list _cdc_ts in route Columns (it's synthetic, not a ScyllaDB column).
				if v, ok := values["_cdc_ts"]; ok {
					row["_cdc_ts"] = v
				}
				if v, ok := values["_cdc_operation"]; ok {
					row["_cdc_operation"] = v
				}
			}
			// If target is fully-qualified (has dot), use as-is; otherwise prepend chDB
			target := route.Target
			if !strings.Contains(target, ".") {
				target = c.chDB + "." + target
			}
			c.writer.Add(target, row, onFlush)
			return
		}
	}
	c.writer.Add(c.chQualifiedName(), values, onFlush)
}

func (c *Consumer) End() error {
	metrics.ConsumersActive.Dec()
	// Save final checkpoint progress with bounded timeout to prevent shutdown hang
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := c.reporter.SaveAndStop(ctx); err != nil {
		return fmt.Errorf("checkpoint save: %w", err)
	}
	return nil
}

// Empty is called when no new rows exist for this stream.
func (c *Consumer) Empty(ctx context.Context, ackTime gocql.UUID) error {
	c.reporter.Update(ackTime)
	return nil
}

// toJSON marshals a value to JSON string. Falls back to fmt.Sprintf on marshal failure
// (should never happen with CQL data, but prevents silent empty-string data loss).
func toJSON(v interface{}) string {
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprintf("%v", v)
	}
	return string(b)
}

func (c *Consumer) shortTableName() string {
	return c.shortName
}

// chQualifiedName returns "database.table" for ClickHouse writer routing.
func (c *Consumer) chQualifiedName() string {
	return c.chDB + "." + c.shortName
}