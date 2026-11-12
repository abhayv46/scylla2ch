package dashboard

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestModifyConfigTables_AddThenRemove(t *testing.T) {
	// Reproduce the exact prod scenario:
	// 1. Start with explicit: []
	// 2. Add pending_rooms
	// 3. Add rooms
	// 4. Try to remove rooms → should work, but fails on prod with "not found"

	dir := t.TempDir()
	cfgFile := filepath.Join(dir, "config.yaml")

	// Start with the prod config template (flow-style empty list)
	initial := `scylla:
  hosts:
    - "localhost:9142"
  keyspace: "chat"
  keyspaces:
    - "chat"
  username: "user"
  password: "pass"
clickhouse:
  host: "localhost"
  port: 9440
  database: "chat"
  username: "default"
  password: ""
tables:
  auto_discover: false
  auto_create: false
  explicit: []
metrics:
  port: 9090
  path: "/metrics"
health:
  port: 8080
`
	if err := os.WriteFile(cfgFile, []byte(initial), 0644); err != nil {
		t.Fatal(err)
	}

	// Step 1: Add pending_rooms
	if err := modifyConfigTables(cfgFile, "pending_rooms", true, "", "chat", 0, 0); err != nil {
		t.Fatalf("add pending_rooms: %v", err)
	}

	// Log what the file looks like after add
	data, _ := os.ReadFile(cfgFile)
	t.Logf("After adding pending_rooms:\n%s", string(data))

	// Step 2: Add rooms
	if err := modifyConfigTables(cfgFile, "rooms", true, "", "chat", 0, 0); err != nil {
		t.Fatalf("add rooms: %v", err)
	}

	data, _ = os.ReadFile(cfgFile)
	t.Logf("After adding rooms:\n%s", string(data))

	// Step 3: Remove rooms — THIS IS WHAT FAILS ON PROD
	if err := modifyConfigTables(cfgFile, "rooms", false, "", "", 0, 0); err != nil {
		t.Fatalf("remove rooms: %v", err)
	}

	data, _ = os.ReadFile(cfgFile)
	t.Logf("After removing rooms:\n%s", string(data))

	// Verify pending_rooms is still there
	if err := modifyConfigTables(cfgFile, "pending_rooms", false, "", "", 0, 0); err != nil {
		t.Fatalf("remove pending_rooms (verify still exists): %v", err)
	}
}

func TestModifyConfigTables_RemoveFromFlowStyle(t *testing.T) {
	// Test with manually written flow-style YAML (like what user showed in vim)
	dir := t.TempDir()
	cfgFile := filepath.Join(dir, "config.yaml")

	initial := `scylla:
  hosts:
    - "localhost:9142"
  keyspace: "chat"
  keyspaces:
    - "chat"
  username: "user"
  password: "pass"
clickhouse:
  host: "localhost"
  port: 9440
  database: "chat"
  username: "default"
  password: ""
tables:
  auto_discover: false
  auto_create: false
  explicit: [{name: "pending_rooms", keyspace: "chat"}, {name: "rooms", keyspace: "chat"}]
metrics:
  port: 9090
  path: "/metrics"
health:
  port: 8080
`
	if err := os.WriteFile(cfgFile, []byte(initial), 0644); err != nil {
		t.Fatal(err)
	}

	// Try to remove rooms
	if err := modifyConfigTables(cfgFile, "rooms", false, "", "", 0, 0); err != nil {
		t.Fatalf("remove rooms from flow-style: %v", err)
	}

	data, _ := os.ReadFile(cfgFile)
	t.Logf("After removing rooms from flow-style:\n%s", string(data))
}

func TestModifyConfigTables_RemoveFromEmptyExplicit(t *testing.T) {
	// This reproduces the actual prod bug: config was overwritten by deploy
	// so explicit: [] is empty, but tables are still running in memory.
	dir := t.TempDir()
	cfgFile := filepath.Join(dir, "config.yaml")

	initial := `scylla:
  hosts:
    - "localhost:9142"
  keyspace: "chat"
  keyspaces:
    - "chat"
  username: "user"
  password: "pass"
clickhouse:
  host: "localhost"
  port: 9440
  database: "chat"
  username: "default"
  password: ""
tables:
  auto_discover: false
  auto_create: false
  explicit: []
metrics:
  port: 9090
  path: "/metrics"
health:
  port: 8080
`
	if err := os.WriteFile(cfgFile, []byte(initial), 0644); err != nil {
		t.Fatal(err)
	}

	// Try to remove rooms from empty list — should fail with helpful message
	err := modifyConfigTables(cfgFile, "rooms", false, "", "", 0, 0)
	if err == nil {
		t.Fatal("expected error removing from empty explicit list")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Fatalf("expected 'not found' error, got: %v", err)
	}
	if !strings.Contains(err.Error(), "0 entries") {
		t.Fatalf("expected '0 entries' in error for empty list, got: %v", err)
	}
	t.Logf("Got expected error: %v", err)
}
