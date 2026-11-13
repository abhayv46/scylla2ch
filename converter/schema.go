// schema.go — ScyllaDB schema introspection and ClickHouse DDL generation.
//
// Powers two features:
//   1. Auto-discover: scans system_schema.tables for CDC-enabled tables
//      (detects *_scylla_cdc_log companion tables)
//   2. Auto-create: generates CREATE TABLE DDL for ClickHouse from ScyllaDB schema
//      (ReplacingMergeTree with _cdc_ts version column)
//
// ScyllaDB system_schema queries:
//   - system_schema.tables → find all tables, detect CDC log companions
//   - system_schema.columns → read column names, types, PK info, positions
//
// CQL compatibility notes (important for ScyllaDB Cloud):
//   - ORDER BY is only allowed on clustering columns — we sort in Go
//   - IN filters on system tables may need ALLOW FILTERING — we filter in Go
//   - These queries are safe on all ScyllaDB versions (OSS and Cloud)
//
// The generated DDL uses:
//   - ReplacingMergeTree(_cdc_ts) — last-write-wins deduplication
//   - ORDER BY (partition_keys, clustering_keys) — matches ScyllaDB PK
//   - Nullable() for non-PK columns (CDC delta rows have sparse columns)
//   - _cdc_ts DateTime64(6) — CDC metadata column for RMT version ordering
package converter

import (
	"context"
	"fmt"
	"log"
	"sort"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/gocql/gocql"

	"scylla2ch/config"
)

// ColumnInfo holds metadata about a ScyllaDB column.
type ColumnInfo struct {
	Name string
	Type string
	IsPK bool
}

// Discoverer scans ScyllaDB system tables to find CDC-enabled tables
// and auto-create corresponding ClickHouse tables.
type Discoverer struct {
	logger *log.Logger
}

func NewDiscoverer(logger *log.Logger) *Discoverer {
	return &Discoverer{logger: logger}
}

// ScanForCDCTables queries system_schema.tables for tables with CDC enabled.
// Returns table names (without keyspace prefix).
func (d *Discoverer) ScanForCDCTables(session *gocql.Session, keyspace string) ([]string, error) {
	// ScyllaDB stores CDC config in the extensions map of system_schema.tables.
	// We can also detect CDC tables by looking for *_scylla_cdc_log companion tables.
	iter := session.Query(
		"SELECT table_name FROM system_schema.tables WHERE keyspace_name = ?",
		keyspace,
	).Iter()

	var tableName string
	cdcLogTables := make(map[string]bool)
	var allTables []string

	for iter.Scan(&tableName) {
		if strings.HasSuffix(tableName, "_scylla_cdc_log") {
			// This is a CDC log table — the base table has CDC enabled
			baseName := strings.TrimSuffix(tableName, "_scylla_cdc_log")
			cdcLogTables[baseName] = true
		}
		allTables = append(allTables, tableName)
	}
	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("scan tables: %w", err)
	}

	// Filter: return only base tables that have a corresponding CDC log table
	var cdcTables []string
	for _, t := range allTables {
		if cdcLogTables[t] && !strings.HasSuffix(t, "_scylla_cdc_log") && t != config.ProgressTable {
			cdcTables = append(cdcTables, t)
		}
	}

	return cdcTables, nil
}

// GetScyllaColumns reads column metadata from system_schema.columns.
// FIX C2: Single query, sort partition/clustering/regular independently by position
// to prevent non-deterministic ORDER BY on composite PK tables (positions are per-kind).
func (d *Discoverer) GetScyllaColumns(session *gocql.Session, keyspace, tableName string) ([]ColumnInfo, error) {
	iter := session.Query(
		"SELECT column_name, type, kind, position FROM system_schema.columns WHERE keyspace_name = ? AND table_name = ?",
		keyspace, tableName,
	).Iter()

	var colName, colType, kind string
	var position int

	type colWithPos struct {
		info ColumnInfo
		kind string
		pos  int
	}
	var partitionKeys, clusteringKeys, regularCols []colWithPos

	for iter.Scan(&colName, &colType, &kind, &position) {
		c := colWithPos{
			info: ColumnInfo{
				Name: colName,
				Type: colType,
				IsPK: kind == "partition_key" || kind == "clustering",
			},
			kind: kind,
			pos:  position,
		}
		switch kind {
		case "partition_key":
			partitionKeys = append(partitionKeys, c)
		case "clustering":
			clusteringKeys = append(clusteringKeys, c)
		default:
			regularCols = append(regularCols, c)
		}
	}
	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("get columns: %w", err)
	}

	// Sort each group independently by position (positions are per-kind in ScyllaDB)
	sort.Slice(partitionKeys, func(i, j int) bool { return partitionKeys[i].pos < partitionKeys[j].pos })
	sort.Slice(clusteringKeys, func(i, j int) bool { return clusteringKeys[i].pos < clusteringKeys[j].pos })
	sort.Slice(regularCols, func(i, j int) bool { return regularCols[i].pos < regularCols[j].pos })

	// Emit: partition keys first, then clustering, then regular — deterministic ORDER BY
	columns := make([]ColumnInfo, 0, len(partitionKeys)+len(clusteringKeys)+len(regularCols))
	for _, c := range partitionKeys {
		columns = append(columns, c.info)
	}
	for _, c := range clusteringKeys {
		columns = append(columns, c.info)
	}
	for _, c := range regularCols {
		columns = append(columns, c.info)
	}

	return columns, nil
}

// GetPKColumns returns the primary key columns in order (partition keys first, then clustering).
func (d *Discoverer) GetPKColumns(session *gocql.Session, keyspace, tableName string) ([]string, error) {
	// Query all columns, filter PK in Go (avoids ORDER BY / IN issues on system tables)
	iter := session.Query(
		"SELECT column_name, kind, position FROM system_schema.columns WHERE keyspace_name = ? AND table_name = ?",
		keyspace, tableName,
	).Iter()

	var colName, kind string
	var position int

	type pkCol struct {
		name     string
		kind     string
		position int
	}
	var partitionKeys []pkCol
	var clusteringKeys []pkCol

	for iter.Scan(&colName, &kind, &position) {
		if kind != "partition_key" && kind != "clustering" {
			continue // skip regular columns
		}
		col := pkCol{name: colName, kind: kind, position: position}
		if kind == "partition_key" {
			partitionKeys = append(partitionKeys, col)
		} else {
			clusteringKeys = append(clusteringKeys, col)
		}
	}
	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("get pk: %w", err)
	}

	// Sort by position within each group
	sort.Slice(partitionKeys, func(i, j int) bool {
		return partitionKeys[i].position < partitionKeys[j].position
	})
	sort.Slice(clusteringKeys, func(i, j int) bool {
		return clusteringKeys[i].position < clusteringKeys[j].position
	})

	// Combine: partition keys first, then clustering keys
	var result []string
	for _, pk := range partitionKeys {
		result = append(result, pk.name)
	}
	for _, ck := range clusteringKeys {
		result = append(result, ck.name)
	}

	return result, nil
}

// AutoCreateCHTable creates a ClickHouse ReplacingMergeTree table from ScyllaDB schema.
func (d *Discoverer) AutoCreateCHTable(
	chConn clickhouse.Conn,
	scyllaSession *gocql.Session,
	keyspace, chDatabase, tableName string,
) error {
	// Get ScyllaDB columns
	columns, err := d.GetScyllaColumns(scyllaSession, keyspace, tableName)
	if err != nil {
		return fmt.Errorf("get schema for %s: %w", tableName, err)
	}

	// Get PK columns for ORDER BY
	pkColumns, err := d.GetPKColumns(scyllaSession, keyspace, tableName)
	if err != nil {
		return fmt.Errorf("get pk for %s: %w", tableName, err)
	}
	if len(pkColumns) == 0 {
		return fmt.Errorf("table %s has no primary key columns", tableName)
	}

	// Build CREATE TABLE DDL
	var ddl strings.Builder
	ddl.WriteString(fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s`.`%s` (\n", chDatabase, tableName))

	for _, col := range columns {
		chType := CQLToClickHouseType(col.Type, col.IsPK)
		ddl.WriteString(fmt.Sprintf("    `%s` %s", col.Name, chType))
		ddl.WriteString(",\n")
	}

	// Add CDC metadata columns
	ddl.WriteString("    `_cdc_ts` DateTime64(6) DEFAULT now64(),\n")
	// FIX #12: Soft delete support — ReplacingMergeTree uses _cdc_is_deleted as the
	// is_deleted column (ClickHouse 23.2+). Rows with _cdc_is_deleted=1 are removed
	// during OPTIMIZE FINAL or background merges instead of staying as stale data.
	ddl.WriteString("    `_cdc_is_deleted` UInt8 DEFAULT 0\n")
	ddl.WriteString(") ENGINE = ReplacingMergeTree(_cdc_ts, _cdc_is_deleted)\n")
	// Backtick-quote PK columns in ORDER BY (consistent with column definitions above)
	quotedPK := make([]string, len(pkColumns))
	for i, pk := range pkColumns {
		quotedPK[i] = "`" + pk + "`"
	}
	ddl.WriteString(fmt.Sprintf("ORDER BY (%s)\n", strings.Join(quotedPK, ", ")))

	ddlStr := ddl.String()
	d.logger.Printf("auto-creating ClickHouse table: %s.%s", chDatabase, tableName)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return chConn.Exec(ctx, ddlStr)
}
