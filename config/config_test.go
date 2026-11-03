package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// ---------- Config Loading ----------

func TestLoad_Defaults(t *testing.T) {
	yaml := `
scylla:
  hosts: ["localhost"]
  keyspace: "test"
  username: "user"
  password: "pass"
clickhouse:
  host: "localhost"
  database: "testdb"
  username: "default"
  password: ""
tables:
  auto_discover: true
`
	cfg := loadFromString(t, yaml)

	if cfg.Batch.Size != 10000 {
		t.Errorf("batch.size default: expected 10000, got %d", cfg.Batch.Size)
	}
	if cfg.Batch.Interval != 5*time.Second {
		t.Errorf("batch.interval default: expected 5s, got %v", cfg.Batch.Interval)
	}
	if cfg.Batch.RetryMax != 3 {
		t.Errorf("batch.retry_max default: expected 3, got %d", cfg.Batch.RetryMax)
	}
	if cfg.Batch.RetryBackoff != 500*time.Millisecond {
		t.Errorf("batch.retry_backoff default: expected 500ms, got %v", cfg.Batch.RetryBackoff)
	}
	if cfg.ClickHouse.Port != 9440 {
		t.Errorf("clickhouse.port default: expected 9440, got %d", cfg.ClickHouse.Port)
	}
	if cfg.ClickHouse.DialTimeout != 2*time.Minute {
		t.Errorf("clickhouse.dial_timeout default: expected 2m, got %v", cfg.ClickHouse.DialTimeout)
	}
	if cfg.ClickHouse.MaxOpenConns != 10 {
		t.Errorf("clickhouse.max_open_conns default: expected 10, got %d", cfg.ClickHouse.MaxOpenConns)
	}
	if cfg.ClickHouse.MaxIdleConns != 5 {
		t.Errorf("clickhouse.max_idle_conns default: expected 5, got %d", cfg.ClickHouse.MaxIdleConns)
	}
	if cfg.CDC.ConfidenceWindow != 5*time.Second {
		t.Errorf("cdc.confidence_window default: expected 5s, got %v", cfg.CDC.ConfidenceWindow)
	}
	if cfg.CDC.QueryWindow != 1*time.Minute {
		t.Errorf("cdc.query_window default: expected 1m, got %v", cfg.CDC.QueryWindow)
	}
	if cfg.CDC.ChangeAgeLimit != 8*time.Hour {
		t.Errorf("cdc.change_age_limit default: expected 8h, got %v", cfg.CDC.ChangeAgeLimit)
	}
	if cfg.CDC.CheckpointInterval != 60*time.Second {
		t.Errorf("cdc.checkpoint_interval default: expected 60s, got %v", cfg.CDC.CheckpointInterval)
	}
	if cfg.Metrics.Port != 9090 {
		t.Errorf("metrics.port default: expected 9090, got %d", cfg.Metrics.Port)
	}
	if cfg.Metrics.Path != "/metrics" {
		t.Errorf("metrics.path default: expected /metrics, got %s", cfg.Metrics.Path)
	}
	if cfg.Health.Port != 8080 {
		t.Errorf("health.port default: expected 8080, got %d", cfg.Health.Port)
	}
}

func TestLoad_CustomValues(t *testing.T) {
	yaml := `
scylla:
  hosts: ["node1", "node2"]
  port: 9142
  keyspace: "prod"
  username: "admin"
  password: "secret"
  tls: true
  dc: "us-east-1"
clickhouse:
  host: "ch.example.com"
  port: 9000
  database: "analytics"
  username: "writer"
  password: "chpass"
  tls: true
batch:
  size: 5000
  interval: "3s"
  retry_max: 5
  retry_backoff: "1s"
cdc:
  mode: "delta"
  confidence_window: "10s"
metrics:
  port: 9091
  path: "/prom"
health:
  port: 8081
tables:
  auto_discover: true
`
	cfg := loadFromString(t, yaml)

	if len(cfg.Scylla.Hosts) != 2 {
		t.Errorf("expected 2 hosts, got %d", len(cfg.Scylla.Hosts))
	}
	if cfg.Scylla.Port != 9142 {
		t.Errorf("expected port 9142, got %d", cfg.Scylla.Port)
	}
	if !cfg.Scylla.TLS {
		t.Error("expected TLS true")
	}
	if cfg.Scylla.DC != "us-east-1" {
		t.Errorf("expected dc us-east-1, got %s", cfg.Scylla.DC)
	}
	if cfg.Batch.Size != 5000 {
		t.Errorf("expected batch size 5000, got %d", cfg.Batch.Size)
	}
	if cfg.Batch.Interval != 3*time.Second {
		t.Errorf("expected 3s, got %v", cfg.Batch.Interval)
	}
	if cfg.Batch.RetryMax != 5 {
		t.Errorf("expected retry_max 5, got %d", cfg.Batch.RetryMax)
	}
	if cfg.Metrics.Port != 9091 {
		t.Errorf("expected 9091, got %d", cfg.Metrics.Port)
	}
	if cfg.Metrics.Path != "/prom" {
		t.Errorf("expected /prom, got %s", cfg.Metrics.Path)
	}
}

func TestLoad_EnvExpansion(t *testing.T) {
	os.Setenv("TEST_SCYLLA2CH_HOST", "env-host.example.com")
	defer os.Unsetenv("TEST_SCYLLA2CH_HOST")

	yaml := `
scylla:
  hosts: ["${TEST_SCYLLA2CH_HOST}"]
  keyspace: "test"
  username: "u"
  password: "p"
clickhouse:
  host: "localhost"
  database: "db"
  username: "default"
  password: ""
tables:
  auto_discover: true
`
	cfg := loadFromString(t, yaml)

	if cfg.Scylla.Hosts[0] != "env-host.example.com" {
		t.Errorf("expected env expansion, got %s", cfg.Scylla.Hosts[0])
	}
}

func TestLoad_CACertBase64(t *testing.T) {
	yaml := `
scylla:
  hosts: ["localhost"]
  keyspace: "test"
  username: "u"
  password: "p"
  ca_cert_base64: "dGVzdA=="
clickhouse:
  host: "localhost"
  database: "db"
  username: "default"
  password: ""
tables:
  auto_discover: true
`
	cfg := loadFromString(t, yaml)

	if cfg.Scylla.CACertBase64 != "dGVzdA==" {
		t.Errorf("expected dGVzdA==, got %s", cfg.Scylla.CACertBase64)
	}
}

func TestLoad_AutoDiscoverInterval_Default(t *testing.T) {
	yaml := `
scylla:
  hosts: ["localhost"]
  keyspace: "test"
  username: "u"
  password: "p"
clickhouse:
  host: "localhost"
  database: "db"
  username: "default"
  password: ""
tables:
  auto_discover: true
`
	cfg := loadFromString(t, yaml)

	if cfg.Tables.DiscoverInterval != 30*time.Second {
		t.Errorf("expected 30s default discover_interval, got %v", cfg.Tables.DiscoverInterval)
	}
}

func TestLoad_AutoDiscoverInterval_Custom(t *testing.T) {
	yaml := `
scylla:
  hosts: ["localhost"]
  keyspace: "test"
  username: "u"
  password: "p"
clickhouse:
  host: "localhost"
  database: "db"
  username: "default"
  password: ""
tables:
  auto_discover: true
  discover_interval: "1m"
`
	cfg := loadFromString(t, yaml)

	if cfg.Tables.DiscoverInterval != 1*time.Minute {
		t.Errorf("expected 1m discover_interval, got %v", cfg.Tables.DiscoverInterval)
	}
}

func TestLoad_AutoDiscoverFalse_NoInterval(t *testing.T) {
	yaml := `
scylla:
  hosts: ["localhost"]
  keyspace: "test"
  username: "u"
  password: "p"
clickhouse:
  host: "localhost"
  database: "db"
  username: "default"
  password: ""
tables:
  auto_discover: false
  explicit:
    - name: "rooms"
`
	cfg := loadFromString(t, yaml)

	// When auto_discover is false, discover_interval should stay 0
	if cfg.Tables.DiscoverInterval != 0 {
		t.Errorf("expected 0 when auto_discover=false, got %v", cfg.Tables.DiscoverInterval)
	}
}

func TestLoad_InvalidYAML(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bad.yaml")
	os.WriteFile(path, []byte("{{invalid yaml"), 0644)

	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for invalid YAML")
	}
}

func TestLoad_MissingFile(t *testing.T) {
	_, err := Load("/nonexistent/path.yaml")
	if err == nil {
		t.Fatal("expected error for missing file")
	}
}

// ---------- TableNames ----------

func TestTableNames(t *testing.T) {
	yaml := `
scylla:
  hosts: ["localhost"]
  keyspace: "chat"
  username: "u"
  password: "p"
clickhouse:
  host: "localhost"
  database: "db"
  username: "default"
  password: ""
tables:
  explicit:
    - name: "rooms"
    - name: "messages"
    - name: "users"
`
	cfg := loadFromString(t, yaml)

	names := cfg.TableNames()
	if len(names) != 3 {
		t.Fatalf("expected 3 tables, got %d", len(names))
	}
	if names[0] != "chat.rooms" {
		t.Errorf("expected chat.rooms, got %s", names[0])
	}
	if names[1] != "chat.messages" {
		t.Errorf("expected chat.messages, got %s", names[1])
	}
}

// ---------- GetTableConfig ----------

func TestGetTableConfig_ExplicitFound(t *testing.T) {
	yaml := `
scylla:
  hosts: ["localhost"]
  keyspace: "test"
  username: "u"
  password: "p"
clickhouse:
  host: "localhost"
  database: "db"
  username: "default"
  password: ""
tables:
  explicit:
    - name: "rooms"
      skip_columns: ["large_blob"]
`
	cfg := loadFromString(t, yaml)

	tc := cfg.GetTableConfig("rooms")
	if tc == nil {
		t.Fatal("expected table config for rooms")
	}
	if !tc.IsColumnSkipped("large_blob") {
		t.Error("expected large_blob to be skipped")
	}
	if tc.IsColumnSkipped("id") {
		t.Error("id should not be skipped")
	}
}

func TestGetTableConfig_OverrideFound(t *testing.T) {
	yaml := `
scylla:
  hosts: ["localhost"]
  keyspace: "test"
  username: "u"
  password: "p"
clickhouse:
  host: "localhost"
  database: "db"
  username: "default"
  password: ""
tables:
  auto_discover: true
  overrides:
    - name: "messages"
      skip_columns: ["body_raw"]
`
	cfg := loadFromString(t, yaml)

	tc := cfg.GetTableConfig("messages")
	if tc == nil {
		t.Fatal("expected override config for messages")
	}
	if !tc.IsColumnSkipped("body_raw") {
		t.Error("expected body_raw to be skipped")
	}
}

func TestGetTableConfig_NotFound(t *testing.T) {
	yaml := `
scylla:
  hosts: ["localhost"]
  keyspace: "test"
  username: "u"
  password: "p"
clickhouse:
  host: "localhost"
  database: "db"
  username: "default"
  password: ""
tables:
  auto_discover: true
`
	cfg := loadFromString(t, yaml)

	tc := cfg.GetTableConfig("nonexistent")
	if tc != nil {
		t.Error("expected nil for nonexistent table")
	}
}

// ---------- RouteConfig.Matches ----------

func TestRouteMatches_EmptyFilter(t *testing.T) {
	r := RouteConfig{Filter: ""}
	if !r.Matches(map[string]interface{}{"a": 1}) {
		t.Error("empty filter should match everything")
	}
}

func TestRouteMatches_IsNotNull_Match(t *testing.T) {
	r := RouteConfig{Filter: "status IS NOT NULL"}
	if !r.Matches(map[string]interface{}{"status": "active"}) {
		t.Error("should match when status is present")
	}
}

func TestRouteMatches_IsNotNull_NoMatch(t *testing.T) {
	r := RouteConfig{Filter: "status IS NOT NULL"}
	if r.Matches(map[string]interface{}{"other": 1}) {
		t.Error("should not match when status is missing")
	}
}

func TestRouteMatches_IsNotNull_NilValue(t *testing.T) {
	r := RouteConfig{Filter: "status IS NOT NULL"}
	if r.Matches(map[string]interface{}{"status": nil}) {
		t.Error("should not match when status is nil")
	}
}

func TestRouteMatches_IsNull_Match(t *testing.T) {
	r := RouteConfig{Filter: "deleted_at IS NULL"}
	if !r.Matches(map[string]interface{}{"id": 1}) {
		t.Error("should match when deleted_at is missing")
	}
}

func TestRouteMatches_IsNull_NilValue(t *testing.T) {
	r := RouteConfig{Filter: "deleted_at IS NULL"}
	if !r.Matches(map[string]interface{}{"deleted_at": nil}) {
		t.Error("should match when deleted_at is nil")
	}
}

func TestRouteMatches_IsNull_NoMatch(t *testing.T) {
	r := RouteConfig{Filter: "deleted_at IS NULL"}
	if r.Matches(map[string]interface{}{"deleted_at": "2026-01-01"}) {
		t.Error("should not match when deleted_at has a value")
	}
}

func TestRouteMatches_AND_BothTrue(t *testing.T) {
	r := RouteConfig{Filter: "a IS NOT NULL AND b IS NOT NULL"}
	row := map[string]interface{}{"a": 1, "b": 2}
	if !r.Matches(row) {
		t.Error("should match when both conditions are true")
	}
}

func TestRouteMatches_AND_OneFalse(t *testing.T) {
	r := RouteConfig{Filter: "a IS NOT NULL AND b IS NOT NULL"}
	row := map[string]interface{}{"a": 1}
	if r.Matches(row) {
		t.Error("should not match when one condition fails")
	}
}

func TestRouteMatches_OR_OneTrue(t *testing.T) {
	r := RouteConfig{Filter: "a IS NOT NULL OR b IS NOT NULL"}
	row := map[string]interface{}{"a": 1}
	if !r.Matches(row) {
		t.Error("should match when one condition is true")
	}
}

func TestRouteMatches_OR_NoneTrue(t *testing.T) {
	r := RouteConfig{Filter: "a IS NOT NULL OR b IS NOT NULL"}
	row := map[string]interface{}{"c": 1}
	if r.Matches(row) {
		t.Error("should not match when no condition is true")
	}
}

// ---------- IsColumnSkipped ----------

func TestIsColumnSkipped(t *testing.T) {
	tc := TableConfig{
		Name:        "test",
		SkipColumns: []string{"col1", "col2"},
	}
	if !tc.IsColumnSkipped("col1") {
		t.Error("col1 should be skipped")
	}
	if !tc.IsColumnSkipped("col2") {
		t.Error("col2 should be skipped")
	}
	if tc.IsColumnSkipped("col3") {
		t.Error("col3 should not be skipped")
	}
}

// ---------- AND/OR precedence edge cases ----------

func TestRouteMatches_AND_AllTrue(t *testing.T) {
	r := RouteConfig{Filter: "a IS NOT NULL AND b IS NOT NULL AND c IS NOT NULL"}
	row := map[string]interface{}{"a": 1, "b": 2, "c": 3}
	if !r.Matches(row) {
		t.Error("should match when all AND conditions true")
	}
}

func TestRouteMatches_AND_LastFalse(t *testing.T) {
	r := RouteConfig{Filter: "a IS NOT NULL AND b IS NOT NULL AND c IS NOT NULL"}
	row := map[string]interface{}{"a": 1, "b": 2}
	if r.Matches(row) {
		t.Error("should not match when last AND condition fails")
	}
}

func TestRouteMatches_OR_AllTrue(t *testing.T) {
	r := RouteConfig{Filter: "a IS NOT NULL OR b IS NOT NULL OR c IS NOT NULL"}
	row := map[string]interface{}{"a": 1, "b": 2, "c": 3}
	if !r.Matches(row) {
		t.Error("should match when all OR conditions true")
	}
}

func TestRouteMatches_OR_LastTrue(t *testing.T) {
	r := RouteConfig{Filter: "a IS NOT NULL OR b IS NOT NULL OR c IS NOT NULL"}
	row := map[string]interface{}{"c": 3}
	if !r.Matches(row) {
		t.Error("should match when last OR condition true")
	}
}

func TestRouteMatches_EmptyRow(t *testing.T) {
	r := RouteConfig{Filter: "x IS NULL"}
	if !r.Matches(map[string]interface{}{}) {
		t.Error("empty row: missing key should match IS NULL")
	}
}

func TestRouteMatches_IsNotNull_EmptyStringIsNotNull(t *testing.T) {
	// Empty string is NOT null — it's a value
	r := RouteConfig{Filter: "name IS NOT NULL"}
	if !r.Matches(map[string]interface{}{"name": ""}) {
		t.Error("empty string should count as NOT NULL")
	}
}

func TestRouteMatches_IsNotNull_ZeroValueIsNotNull(t *testing.T) {
	r := RouteConfig{Filter: "count IS NOT NULL"}
	if !r.Matches(map[string]interface{}{"count": 0}) {
		t.Error("zero should count as NOT NULL")
	}
}

func TestRouteMatches_IsNotNull_FalseIsNotNull(t *testing.T) {
	r := RouteConfig{Filter: "active IS NOT NULL"}
	if !r.Matches(map[string]interface{}{"active": false}) {
		t.Error("false should count as NOT NULL")
	}
}

func TestRouteMatches_UnknownCondition_DefaultsTrue(t *testing.T) {
	// Unknown filter syntax — evalCondition returns true
	r := RouteConfig{Filter: "something_unknown"}
	if !r.Matches(map[string]interface{}{"a": 1}) {
		t.Error("unknown condition should default to true")
	}
}

// ---------- Routes with columns ----------

func TestTableConfig_Routes(t *testing.T) {
	yaml := `
scylla:
  hosts: ["localhost"]
  keyspace: "test"
  username: "u"
  password: "p"
clickhouse:
  host: "localhost"
  database: "db"
  username: "default"
  password: ""
tables:
  explicit:
    - name: "events"
      routes:
        - target: "click_events"
          filter: "event_type IS NOT NULL"
          columns: [user_id, event_type, created_at]
        - target: "page_events"
          filter: "page_url IS NOT NULL"
          columns: [user_id, page_url]
`
	cfg := loadFromString(t, yaml)

	tc := cfg.GetTableConfig("events")
	if tc == nil {
		t.Fatal("expected events config")
	}
	if len(tc.Routes) != 2 {
		t.Fatalf("expected 2 routes, got %d", len(tc.Routes))
	}
	if tc.Routes[0].Target != "click_events" {
		t.Errorf("expected click_events, got %s", tc.Routes[0].Target)
	}
	if len(tc.Routes[0].Columns) != 3 {
		t.Errorf("expected 3 columns, got %d", len(tc.Routes[0].Columns))
	}

	// Test first route matches
	row1 := map[string]interface{}{"event_type": "click", "user_id": 1}
	if !tc.Routes[0].Matches(row1) {
		t.Error("click_events route should match row with event_type")
	}

	// Test second route matches when first doesn't
	row2 := map[string]interface{}{"page_url": "/home", "user_id": 2}
	if !tc.Routes[1].Matches(row2) {
		t.Error("page_events route should match row with page_url")
	}
}

// ---------- TableNames empty ----------

func TestTableNames_Empty(t *testing.T) {
	yaml := `
scylla:
  hosts: ["localhost"]
  keyspace: "test"
  username: "u"
  password: "p"
clickhouse:
  host: "localhost"
  database: "db"
  username: "default"
  password: ""
tables:
  auto_discover: true
`
	cfg := loadFromString(t, yaml)
	names := cfg.TableNames()
	if len(names) != 0 {
		t.Fatalf("expected 0 tables, got %d", len(names))
	}
}

// ---------- IsColumnSkipped empty ----------

func TestIsColumnSkipped_EmptyList(t *testing.T) {
	tc := TableConfig{Name: "test"}
	if tc.IsColumnSkipped("anything") {
		t.Error("nothing should be skipped with empty skip list")
	}
}

// ---------- Explicit takes precedence over override ----------

func TestGetTableConfig_ExplicitBeforeOverride(t *testing.T) {
	yaml := `
scylla:
  hosts: ["localhost"]
  keyspace: "test"
  username: "u"
  password: "p"
clickhouse:
  host: "localhost"
  database: "db"
  username: "default"
  password: ""
tables:
  explicit:
    - name: "rooms"
      skip_columns: ["from_explicit"]
  overrides:
    - name: "rooms"
      skip_columns: ["from_override"]
`
	cfg := loadFromString(t, yaml)
	tc := cfg.GetTableConfig("rooms")
	if tc == nil {
		t.Fatal("expected config")
	}
	// Explicit should win
	if !tc.IsColumnSkipped("from_explicit") {
		t.Error("explicit config should be returned first")
	}
	if tc.IsColumnSkipped("from_override") {
		t.Error("override config should not be returned when explicit exists")
	}
}

// ---------- Validation: missing required fields ----------

func TestLoad_MissingHosts(t *testing.T) {
	yaml := `
scylla:
  hosts: []
  keyspace: "test"
clickhouse:
  host: "localhost"
  database: "db"
tables:
  auto_discover: true
`
	_, err := loadFromStringErr(t, yaml)
	if err == nil {
		t.Fatal("expected error for empty hosts")
	}
}

func TestLoad_MissingKeyspace(t *testing.T) {
	yaml := `
scylla:
  hosts: ["localhost"]
clickhouse:
  host: "localhost"
  database: "db"
tables:
  auto_discover: true
`
	_, err := loadFromStringErr(t, yaml)
	if err == nil {
		t.Fatal("expected error for missing keyspace")
	}
}

func TestLoad_MissingCHHost(t *testing.T) {
	yaml := `
scylla:
  hosts: ["localhost"]
  keyspace: "test"
clickhouse:
  host: ""
  database: "db"
tables:
  auto_discover: true
`
	_, err := loadFromStringErr(t, yaml)
	if err == nil {
		t.Fatal("expected error for empty clickhouse host")
	}
}

func TestLoad_MissingCHDatabase(t *testing.T) {
	yaml := `
scylla:
  hosts: ["localhost"]
  keyspace: "test"
clickhouse:
  host: "localhost"
  database: ""
tables:
  auto_discover: true
`
	_, err := loadFromStringErr(t, yaml)
	if err == nil {
		t.Fatal("expected error for empty clickhouse database")
	}
}

// ---------- Thread-safety ----------

func TestGetTableConfig_ThreadSafe(t *testing.T) {
	yaml := `
scylla:
  hosts: ["localhost"]
  keyspace: "test"
  username: "u"
  password: "p"
clickhouse:
  host: "localhost"
  database: "db"
  username: "default"
  password: ""
tables:
  explicit:
    - name: "rooms"
      skip_columns: ["blob"]
    - name: "messages"
  overrides:
    - name: "events"
`
	cfg := loadFromString(t, yaml)

	// Concurrent reads must not race
	done := make(chan struct{})
	for i := 0; i < 100; i++ {
		go func() {
			_ = cfg.GetTableConfig("rooms")
			_ = cfg.GetTableConfig("messages")
			_ = cfg.GetTableConfig("events")
			_ = cfg.GetTableConfig("nonexistent")
			done <- struct{}{}
		}()
	}
	for i := 0; i < 100; i++ {
		<-done
	}
}

func TestIsColumnSkipped_ThreadSafe(t *testing.T) {
	tc := &TableConfig{
		Name:        "test",
		SkipColumns: []string{"col1", "col2", "col3"},
	}

	done := make(chan struct{})
	for i := 0; i < 100; i++ {
		go func() {
			_ = tc.IsColumnSkipped("col1")
			_ = tc.IsColumnSkipped("col2")
			_ = tc.IsColumnSkipped("col3")
			_ = tc.IsColumnSkipped("col4")
			done <- struct{}{}
		}()
	}
	for i := 0; i < 100; i++ {
		<-done
	}
}

// ---------- ProgressTable constant ----------

func TestProgressTableConstant(t *testing.T) {
	if ProgressTable != "scylla2ch_progress" {
		t.Errorf("expected scylla2ch_progress, got %s", ProgressTable)
	}
}

// ---------- TablesDir (drop-in directory like supervisor conf.d/) ----------

func TestLoad_TablesDir(t *testing.T) {
	dir := t.TempDir()

	// Create tables.d/ with two table files
	tablesDir := filepath.Join(dir, "tables.d")
	os.MkdirAll(tablesDir, 0755)
	os.WriteFile(filepath.Join(tablesDir, "messages.yaml"), []byte(`name: "messages"`), 0644)
	os.WriteFile(filepath.Join(tablesDir, "users.yaml"), []byte("name: \"users\"\nskip_columns: [\"avatar\"]"), 0644)
	// .example files should be ignored (not .yaml/.yml)
	os.WriteFile(filepath.Join(tablesDir, "events.yaml.example"), []byte(`name: "events"`), 0644)

	cfgYAML := `
scylla:
  hosts: ["localhost"]
  keyspace: "chat"
  username: "u"
  password: "p"
clickhouse:
  host: "localhost"
  database: "db"
  username: "default"
  password: ""
tables:
  auto_discover: false
  tables_dir: "tables.d"
`
	cfgPath := filepath.Join(dir, "config.yaml")
	os.WriteFile(cfgPath, []byte(cfgYAML), 0644)

	cfg, err := Load(cfgPath)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	names := cfg.TableNames()
	if len(names) != 2 {
		t.Fatalf("expected 2 tables from tables.d, got %d: %v", len(names), names)
	}
	// Sorted alphabetically: messages.yaml before users.yaml
	if names[0] != "chat.messages" {
		t.Errorf("expected chat.messages, got %s", names[0])
	}
	if names[1] != "chat.users" {
		t.Errorf("expected chat.users, got %s", names[1])
	}

	// Verify skip_columns from users.yaml
	tc := cfg.GetTableConfig("users")
	if tc == nil {
		t.Fatal("expected table config for users")
	}
	if !tc.IsColumnSkipped("avatar") {
		t.Error("expected avatar to be skipped")
	}
}

func TestLoad_TablesDir_MergedWithExplicit(t *testing.T) {
	dir := t.TempDir()

	tablesDir := filepath.Join(dir, "tables.d")
	os.MkdirAll(tablesDir, 0755)
	os.WriteFile(filepath.Join(tablesDir, "payments.yaml"), []byte(`name: "payments"`), 0644)

	cfgYAML := `
scylla:
  hosts: ["localhost"]
  keyspace: "chat"
  username: "u"
  password: "p"
clickhouse:
  host: "localhost"
  database: "db"
  username: "default"
  password: ""
tables:
  auto_discover: false
  tables_dir: "tables.d"
  explicit:
    - name: "messages"
`
	cfgPath := filepath.Join(dir, "config.yaml")
	os.WriteFile(cfgPath, []byte(cfgYAML), 0644)

	cfg, err := Load(cfgPath)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	names := cfg.TableNames()
	if len(names) != 2 {
		t.Fatalf("expected 2 tables (1 explicit + 1 drop-in), got %d: %v", len(names), names)
	}
	if names[0] != "chat.messages" {
		t.Errorf("expected chat.messages first (explicit), got %s", names[0])
	}
	if names[1] != "chat.payments" {
		t.Errorf("expected chat.payments second (drop-in), got %s", names[1])
	}
}

func TestLoad_TablesDir_EmptyFileSkipped(t *testing.T) {
	dir := t.TempDir()

	tablesDir := filepath.Join(dir, "tables.d")
	os.MkdirAll(tablesDir, 0755)
	os.WriteFile(filepath.Join(tablesDir, "empty.yaml"), []byte("# just a comment\n"), 0644)
	os.WriteFile(filepath.Join(tablesDir, "real.yaml"), []byte(`name: "users"`), 0644)

	cfgYAML := `
scylla:
  hosts: ["localhost"]
  keyspace: "test"
  username: "u"
  password: "p"
clickhouse:
  host: "localhost"
  database: "db"
  username: "default"
  password: ""
tables:
  auto_discover: false
  tables_dir: "tables.d"
`
	cfgPath := filepath.Join(dir, "config.yaml")
	os.WriteFile(cfgPath, []byte(cfgYAML), 0644)

	cfg, err := Load(cfgPath)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	names := cfg.TableNames()
	if len(names) != 1 {
		t.Fatalf("expected 1 table (empty file skipped), got %d: %v", len(names), names)
	}
}

func TestLoad_TablesDir_BadDir(t *testing.T) {
	dir := t.TempDir()
	cfgYAML := `
scylla:
  hosts: ["localhost"]
  keyspace: "test"
  username: "u"
  password: "p"
clickhouse:
  host: "localhost"
  database: "db"
  username: "default"
  password: ""
tables:
  auto_discover: false
  tables_dir: "nonexistent_dir"
`
	cfgPath := filepath.Join(dir, "config.yaml")
	os.WriteFile(cfgPath, []byte(cfgYAML), 0644)

	_, err := Load(cfgPath)
	if err == nil {
		t.Fatal("expected error for nonexistent tables_dir")
	}
}

func TestLoad_TablesDir_EnvExpansion(t *testing.T) {
	os.Setenv("TEST_TABLE_NAME", "orders")
	defer os.Unsetenv("TEST_TABLE_NAME")

	dir := t.TempDir()
	tablesDir := filepath.Join(dir, "tables.d")
	os.MkdirAll(tablesDir, 0755)
	os.WriteFile(filepath.Join(tablesDir, "orders.yaml"), []byte(`name: "${TEST_TABLE_NAME}"`), 0644)

	cfgYAML := `
scylla:
  hosts: ["localhost"]
  keyspace: "test"
  username: "u"
  password: "p"
clickhouse:
  host: "localhost"
  database: "db"
  username: "default"
  password: ""
tables:
  auto_discover: false
  tables_dir: "tables.d"
`
	cfgPath := filepath.Join(dir, "config.yaml")
	os.WriteFile(cfgPath, []byte(cfgYAML), 0644)

	cfg, err := Load(cfgPath)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	names := cfg.TableNames()
	if len(names) != 1 || names[0] != "test.orders" {
		t.Errorf("expected [test.orders], got %v", names)
	}
}

// ---------- Helper ----------

func loadFromStringErr(t *testing.T, yamlContent string) (*Config, error) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(yamlContent), 0644); err != nil {
		t.Fatal(err)
	}
	return Load(path)
}

func loadFromString(t *testing.T, yamlContent string) *Config {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(yamlContent), 0644); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}
