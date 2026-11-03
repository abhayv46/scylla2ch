// Package config loads and validates scylla2ch YAML configuration.
//
// Configuration supports ${ENV_VAR} expansion in all string values — this means
// secrets (passwords, CA certs) never need to be in the config file.
//
// Sections:
//   - ScyllaConfig:     ScyllaDB connection (hosts, TLS, CA cert, datacenter)
//   - ClickHouseConfig: ClickHouse connection (host, TLS, pool sizes)
//   - CDCConfig:        CDC tuning (confidence window, query window, age limit)
//   - BatchConfig:      Flush tuning (batch size, interval, retry policy)
//   - TablesConfig:     Table discovery (auto-discover, auto-create, hot-reload interval)
//   - MetricsConfig:    Prometheus endpoint (port, path)
//   - HealthConfig:     Health check endpoint (port)
//
// Sensible defaults are applied for all optional fields (see Load()).
//
// Multi-target routing: a single ScyllaDB table can be split into N ClickHouse
// tables using RouteConfig with IS NOT NULL / IS NULL filter expressions.
//
// Example: config.yaml.example
package config

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"gopkg.in/yaml.v3"
)

// ProgressTable is the ScyllaDB table name used by scylla-cdc-go for checkpoint tracking.
const ProgressTable = "scylla2ch_progress"

type Config struct {
	Scylla     ScyllaConfig     `yaml:"scylla"`
	ClickHouse ClickHouseConfig `yaml:"clickhouse"`
	CDC        CDCConfig        `yaml:"cdc"`
	Batch      BatchConfig      `yaml:"batch"`
	Tables     TablesConfig     `yaml:"tables"`
	Metrics    MetricsConfig    `yaml:"metrics"`
	Health     HealthConfig     `yaml:"health"`

	tableIndex     map[string]*TableConfig // cached O(1) lookup, built on first GetTableConfig call
	tableIndexOnce sync.Once
}

type ScyllaConfig struct {
	Hosts        []string `yaml:"hosts"`
	Port         int      `yaml:"port"`

	Keyspace     string   `yaml:"keyspace"`              // single keyspace (backward compat)
	Keyspaces    []string `yaml:"keyspaces,omitempty"`    // multiple keyspaces — one session per keyspace
	Username     string   `yaml:"username"`
	Password     string   `yaml:"password"`
	TLS           bool     `yaml:"tls"`
	TLSSkipVerify bool     `yaml:"tls_skip_verify"` // skip CA verification (dev only — TLS still encrypted)
	CACertPath    string   `yaml:"ca_cert_path"`    // path to CA cert PEM file
	CACertBase64  string   `yaml:"ca_cert_base64"`  // base64-encoded CA cert PEM (for K8s secrets / env vars)
	DC            string   `yaml:"dc"`
	Consistency   string   `yaml:"consistency"`
}

// AllKeyspaces returns the list of keyspaces to stream.
// Supports both single keyspace (backward compat) and multi-keyspace configs.
// If both keyspace and keyspaces are set, keyspaces wins.
func (s *ScyllaConfig) AllKeyspaces() []string {
	if len(s.Keyspaces) > 0 {
		return s.Keyspaces
	}
	if s.Keyspace != "" {
		return []string{s.Keyspace}
	}
	return nil
}

type ClickHouseConfig struct {
	Host         string        `yaml:"host"`
	Port         int           `yaml:"port"`
	Protocol     string        `yaml:"protocol"`
	Database     string        `yaml:"database"`
	Username     string        `yaml:"username"`
	Password     string        `yaml:"password"`
	TLS          bool          `yaml:"tls"`
	Compression  string        `yaml:"compression"`
	DialTimeout  time.Duration `yaml:"dial_timeout"`
	MaxOpenConns int           `yaml:"max_open_conns"`
	MaxIdleConns int           `yaml:"max_idle_conns"`
}

type CDCConfig struct {
	Mode               string        `yaml:"mode"`
	ConfidenceWindow   time.Duration `yaml:"confidence_window"`
	QueryWindow        time.Duration `yaml:"query_window"`
	ChangeAgeLimit     time.Duration `yaml:"change_age_limit"`
	CheckpointInterval time.Duration `yaml:"checkpoint_interval"`
}

type BatchConfig struct {
	Size         int           `yaml:"size"`
	Interval     time.Duration `yaml:"interval"`
	RetryMax     int           `yaml:"retry_max"`
	RetryBackoff time.Duration `yaml:"retry_backoff"`
}

type TablesConfig struct {
	AutoDiscover     bool          `yaml:"auto_discover"`
	AutoCreate       bool          `yaml:"auto_create"`
	DiscoverInterval time.Duration `yaml:"discover_interval"` // how often to re-scan for new CDC tables (hot-reload, 0 = disabled)
	TablesDir        string        `yaml:"tables_dir"`        // drop-in directory: each *.yaml file = one TableConfig (like supervisor conf.d/)
	Explicit         []TableConfig `yaml:"explicit,omitempty"`
	Overrides        []TableConfig `yaml:"overrides,omitempty"`
}

type TableConfig struct {
	Name               string        `yaml:"name"`
	Routes             []RouteConfig `yaml:"routes,omitempty"`
	SkipColumns        []string      `yaml:"skip_columns,omitempty"`
	skipColumnsMap     map[string]bool // cached O(1) lookup, built on first IsColumnSkipped call
	skipColumnsMapOnce sync.Once
}

type RouteConfig struct {
	Target  string   `yaml:"target"`
	Filter  string   `yaml:"filter"`
	Columns []string `yaml:"columns"`
}

type MetricsConfig struct {
	Port int    `yaml:"port"`
	Path string `yaml:"path"`
}

type HealthConfig struct {
	Port int `yaml:"port"`
}

// Matches evaluates a simple filter expression against a row.
// Supports: "col IS NOT NULL", "col IS NULL", AND, OR.
func (r *RouteConfig) Matches(row map[string]interface{}) bool {
	filter := r.Filter
	if filter == "" {
		return true
	}

	if strings.Contains(filter, " AND ") {
		parts := strings.Split(filter, " AND ")
		for _, part := range parts {
			if !evalCondition(strings.TrimSpace(part), row) {
				return false
			}
		}
		return true
	}

	if strings.Contains(filter, " OR ") {
		parts := strings.Split(filter, " OR ")
		for _, part := range parts {
			if evalCondition(strings.TrimSpace(part), row) {
				return true
			}
		}
		return false
	}

	return evalCondition(filter, row)
}

func evalCondition(cond string, row map[string]interface{}) bool {
	if strings.HasSuffix(cond, "IS NOT NULL") {
		col := strings.TrimSpace(strings.TrimSuffix(cond, "IS NOT NULL"))
		val, exists := row[col]
		return exists && val != nil
	}
	if strings.HasSuffix(cond, "IS NULL") {
		col := strings.TrimSpace(strings.TrimSuffix(cond, "IS NULL"))
		val, exists := row[col]
		return !exists || val == nil
	}
	return true
}

// GetTableConfig returns the config for a specific table name.
// Checks explicit tables first, then overrides.
// Uses a cached map for O(1) lookup (called per CDC row on the hot path).
func (c *Config) GetTableConfig(name string) *TableConfig {
	c.tableIndexOnce.Do(func() {
		c.tableIndex = make(map[string]*TableConfig, len(c.Tables.Explicit)+len(c.Tables.Overrides))
		// Overrides first so explicit takes precedence (overwrite in map)
		for i := range c.Tables.Overrides {
			c.tableIndex[c.Tables.Overrides[i].Name] = &c.Tables.Overrides[i]
		}
		for i := range c.Tables.Explicit {
			c.tableIndex[c.Tables.Explicit[i].Name] = &c.Tables.Explicit[i]
		}
	})
	return c.tableIndex[name]
}

// TableNames returns the list of fully-qualified table names (keyspace.table).
// For multi-keyspace configs, returns tables for ALL keyspaces.
func (c *Config) TableNames() []string {
	keyspaces := c.Scylla.AllKeyspaces()
	names := make([]string, 0, len(c.Tables.Explicit)*len(keyspaces))
	for _, ks := range keyspaces {
		for i := range c.Tables.Explicit {
			names = append(names, fmt.Sprintf("%s.%s", ks, c.Tables.Explicit[i].Name))
		}
	}
	return names
}

// IsColumnSkipped returns true if a column should be skipped for a given table.
// Uses a cached map for O(1) lookup on the hot path (called per column per CDC row).
func (tc *TableConfig) IsColumnSkipped(col string) bool {
	tc.skipColumnsMapOnce.Do(func() {
		if len(tc.SkipColumns) > 0 {
			tc.skipColumnsMap = make(map[string]bool, len(tc.SkipColumns))
			for _, s := range tc.SkipColumns {
				tc.skipColumnsMap[s] = true
			}
		}
	})
	return tc.skipColumnsMap[col]
}

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}

	// Expand environment variables: ${VAR_NAME}
	expanded := os.ExpandEnv(string(data))

	var cfg Config
	if err := yaml.Unmarshal([]byte(expanded), &cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}

	// Load drop-in table configs from tables_dir (like supervisor conf.d/*.conf)
	// Each *.yaml file in tables_dir = one TableConfig, merged into Explicit list.
	if cfg.Tables.TablesDir != "" {
		tablesDir := cfg.Tables.TablesDir
		// Resolve relative paths against config file's directory
		if !filepath.IsAbs(tablesDir) {
			tablesDir = filepath.Join(filepath.Dir(path), tablesDir)
		}

		dropIns, err := loadTablesDir(tablesDir)
		if err != nil {
			return nil, fmt.Errorf("tables_dir %s: %w", tablesDir, err)
		}
		cfg.Tables.Explicit = append(cfg.Tables.Explicit, dropIns...)
	}

	// Defaults
	if cfg.Batch.Size == 0 {
		cfg.Batch.Size = 10000
	}
	if cfg.Batch.Interval == 0 {
		cfg.Batch.Interval = 5 * time.Second
	}
	if cfg.Batch.RetryMax == 0 {
		cfg.Batch.RetryMax = 3
	}
	if cfg.Batch.RetryBackoff == 0 {
		cfg.Batch.RetryBackoff = 500 * time.Millisecond
	}
	if cfg.ClickHouse.Port == 0 {
		cfg.ClickHouse.Port = 9440
	}
	if cfg.ClickHouse.DialTimeout == 0 {
		cfg.ClickHouse.DialTimeout = 2 * time.Minute
	}
	if cfg.ClickHouse.MaxOpenConns == 0 {
		cfg.ClickHouse.MaxOpenConns = 10
	}
	if cfg.ClickHouse.MaxIdleConns == 0 {
		cfg.ClickHouse.MaxIdleConns = 5
	}
	if cfg.CDC.ConfidenceWindow == 0 {
		cfg.CDC.ConfidenceWindow = 5 * time.Second
	}
	if cfg.CDC.QueryWindow == 0 {
		cfg.CDC.QueryWindow = 1 * time.Minute
	}
	if cfg.CDC.ChangeAgeLimit == 0 {
		cfg.CDC.ChangeAgeLimit = 8 * time.Hour
	}
	if cfg.CDC.CheckpointInterval == 0 {
		cfg.CDC.CheckpointInterval = 60 * time.Second
	}
	if cfg.Metrics.Port == 0 {
		cfg.Metrics.Port = 9090
	}
	if cfg.Metrics.Path == "" {
		cfg.Metrics.Path = "/metrics"
	}
	if cfg.Health.Port == 0 {
		cfg.Health.Port = 8080
	}
	// Hot-reload: default 30s scan interval when auto_discover is enabled
	if cfg.Tables.AutoDiscover && cfg.Tables.DiscoverInterval == 0 {
		cfg.Tables.DiscoverInterval = 30 * time.Second
	}

	// Validate required fields
	if len(cfg.Scylla.Hosts) == 0 {
		return nil, fmt.Errorf("scylla.hosts is required")
	}
	if len(cfg.Scylla.AllKeyspaces()) == 0 {
		return nil, fmt.Errorf("scylla.keyspace or scylla.keyspaces is required")
	}
	if cfg.ClickHouse.Host == "" {
		return nil, fmt.Errorf("clickhouse.host is required")
	}
	if cfg.ClickHouse.Database == "" {
		return nil, fmt.Errorf("clickhouse.database is required")
	}
	if !cfg.Tables.AutoDiscover && len(cfg.Tables.Explicit) == 0 {
		return nil, fmt.Errorf("either tables.auto_discover, tables.explicit, or tables.tables_dir is required")
	}

	return &cfg, nil
}

// loadTablesDir reads all *.yaml / *.yml files from a directory and parses
// each as a TableConfig. Like supervisor's conf.d/*.conf — one file per table.
//
// Files are loaded in sorted order (deterministic, alphabetical).
// Empty files or files with only comments are silently skipped.
func loadTablesDir(dir string) ([]TableConfig, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("read dir: %w", err)
	}

	// Collect and sort YAML files
	var files []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		ext := strings.ToLower(filepath.Ext(e.Name()))
		if ext == ".yaml" || ext == ".yml" {
			files = append(files, filepath.Join(dir, e.Name()))
		}
	}
	sort.Strings(files)

	var tables []TableConfig
	for _, f := range files {
		data, err := os.ReadFile(f)
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", f, err)
		}

		expanded := os.ExpandEnv(string(data))

		// Unmarshal directly into slice element to avoid copying sync.Once
		tables = append(tables, TableConfig{})
		tc := &tables[len(tables)-1]
		if err := yaml.Unmarshal([]byte(expanded), tc); err != nil {
			tables = tables[:len(tables)-1]
			return nil, fmt.Errorf("parse %s: %w", f, err)
		}

		// Skip empty/comment-only files
		if tc.Name == "" {
			tables = tables[:len(tables)-1]
		}
	}

	return tables, nil
}
