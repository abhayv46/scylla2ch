// Package dashboard provides a built-in web UI for scylla2ch.
//
// Serves on the health port (8080) with:
//   - GET  /           → embedded single-page dashboard (login-gated)
//   - GET  /health     → plain "ok" (Docker healthcheck, no auth)
//   - GET  /api/status → JSON: tables, uptime, version, config summary
//   - GET  /api/metrics → JSON: parsed prometheus metrics per table
//   - POST /api/tables → add table to config + SIGHUP reload
//   - DELETE /api/tables?name=X → remove table from config + SIGHUP reload
//   - POST /api/login  → authenticate, set session cookie
//
// Auth: basic credentials from env SCYLLA2CH_DASHBOARD_USER / SCYLLA2CH_DASHBOARD_PASS
// (or config yaml). If not set, dashboard returns 403 "credentials not configured".
package dashboard

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"embed"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/gocql/gocql"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"gopkg.in/yaml.v3"

	"scylla2ch/config"
)

//go:embed index.html
var indexHTML embed.FS

// CHTableChecker abstracts the ClickHouse writer's TableExists method
// so the dashboard package doesn't import the writer package directly.
type CHTableChecker interface {
	TableExists(ctx context.Context, table string) (bool, error)
}

// State holds references to live process state for the JSON API.
type State struct {
	Running    *map[string]bool
	RunningMu  *sync.Mutex
	Config     *config.Config
	ConfigFile string
	StartTime  time.Time
	Version    string
	Logger     *log.Logger

	// For pre-flight validation when adding tables
	SessionMap map[string]*gocql.Session // keyspace → ScyllaDB session
	CHDBMap    map[string]string         // keyspace → ClickHouse database
	CHChecker  CHTableChecker            // checks ClickHouse table existence
}

// session store — token → {expiry, role}
type sessionEntry struct {
	expiry time.Time
	role   string // "owner" or "viewer"
}

var (
	sessions   = make(map[string]sessionEntry)
	sessionsMu sync.Mutex
)

// login rate limiting — IP → last failed attempt timestamps
var (
	loginAttempts   = make(map[string][]time.Time)
	loginAttemptsMu sync.Mutex
)

const sessionTTL = 24 * time.Hour
const sessionCookie = "scylla2ch_session"
const maxLoginAttempts = 5           // max failed attempts per IP
const loginWindow = 5 * time.Minute  // within this window
const maxSessions = 100              // prevent unbounded growth

func genToken() string {
	b := make([]byte, 32)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// sessionRole returns the role for a valid session, or "" if invalid/expired.
func sessionRole(r *http.Request) string {
	c, err := r.Cookie(sessionCookie)
	if err != nil {
		return ""
	}
	sessionsMu.Lock()
	entry, ok := sessions[c.Value]
	sessionsMu.Unlock()
	if ok && time.Now().Before(entry.expiry) {
		return entry.role
	}
	return ""
}

func validSession(r *http.Request) bool {
	return sessionRole(r) != ""
}

// cleanExpiredSessions removes expired sessions to prevent unbounded map growth.
func cleanExpiredSessions() {
	now := time.Now()
	sessionsMu.Lock()
	for k, entry := range sessions {
		if now.After(entry.expiry) {
			delete(sessions, k)
		}
	}
	sessionsMu.Unlock()
}

// isRateLimited returns true if the IP has exceeded login attempts.
func isRateLimited(ip string) bool {
	loginAttemptsMu.Lock()
	defer loginAttemptsMu.Unlock()

	now := time.Now()
	cutoff := now.Add(-loginWindow)

	// Clean old attempts
	attempts := loginAttempts[ip]
	var recent []time.Time
	for _, t := range attempts {
		if t.After(cutoff) {
			recent = append(recent, t)
		}
	}
	loginAttempts[ip] = recent
	return len(recent) >= maxLoginAttempts
}

func recordFailedLogin(ip string) {
	loginAttemptsMu.Lock()
	loginAttempts[ip] = append(loginAttempts[ip], time.Now())
	loginAttemptsMu.Unlock()
}

// cleanExpiredLoginAttempts removes stale rate-limit entries to prevent unbounded map growth.
func cleanExpiredLoginAttempts() {
	cutoff := time.Now().Add(-loginWindow)
	loginAttemptsMu.Lock()
	for ip, attempts := range loginAttempts {
		var recent []time.Time
		for _, t := range attempts {
			if t.After(cutoff) {
				recent = append(recent, t)
			}
		}
		if len(recent) == 0 {
			delete(loginAttempts, ip)
		} else {
			loginAttempts[ip] = recent
		}
	}
	loginAttemptsMu.Unlock()
}

func getCredentials(cfg *config.Config) (string, string) {
	user := os.Getenv("SCYLLA2CH_DASHBOARD_USER")
	pass := os.Getenv("SCYLLA2CH_DASHBOARD_PASS")
	if user == "" {
		user = cfg.Dashboard.User
	}
	if pass == "" {
		pass = cfg.Dashboard.Pass
	}
	return user, pass
}

func getViewerCredentials(cfg *config.Config) (string, string) {
	user := os.Getenv("SCYLLA2CH_DASHBOARD_VIEWER_USER")
	pass := os.Getenv("SCYLLA2CH_DASHBOARD_VIEWER_PASS")
	if user == "" {
		user = cfg.Dashboard.ViewerUser
	}
	if pass == "" {
		pass = cfg.Dashboard.ViewerPass
	}
	return user, pass
}

// NewMux returns an http.Handler with all dashboard routes.
func NewMux(s *State) http.Handler {
	mux := http.NewServeMux()

	// Health — no auth, Docker healthcheck
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	// Periodic session + login attempts cleanup (every 10 minutes)
	go func() {
		for range time.Tick(10 * time.Minute) {
			cleanExpiredSessions()
			cleanExpiredLoginAttempts()
		}
	}()

	// Login
	mux.HandleFunc("/api/login", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		clientIP := r.RemoteAddr
		if fwd := r.Header.Get("X-Forwarded-For"); fwd != "" {
			clientIP = strings.SplitN(fwd, ",", 2)[0]
		}

		if isRateLimited(clientIP) {
			writeJSON(w, http.StatusTooManyRequests, map[string]interface{}{
				"ok":    false,
				"error": "too many login attempts, try again later",
			})
			return
		}

		ownerUser, ownerPass := getCredentials(s.Config)
		viewerUser, viewerPass := getViewerCredentials(s.Config)
		if ownerUser == "" || ownerPass == "" {
			writeJSON(w, http.StatusForbidden, map[string]interface{}{
				"ok":    false,
				"error": "dashboard credentials not configured — set SCYLLA2CH_DASHBOARD_USER and SCYLLA2CH_DASHBOARD_PASS",
			})
			return
		}

		var req struct {
			User string `json:"user"`
			Pass string `json:"pass"`
		}
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1024)).Decode(&req); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]interface{}{"ok": false, "error": "invalid JSON"})
			return
		}

		// Check owner credentials first, then viewer
		role := ""
		if subtle.ConstantTimeCompare([]byte(req.User), []byte(ownerUser)) == 1 &&
			subtle.ConstantTimeCompare([]byte(req.Pass), []byte(ownerPass)) == 1 {
			role = "owner"
		} else if viewerUser != "" && viewerPass != "" &&
			subtle.ConstantTimeCompare([]byte(req.User), []byte(viewerUser)) == 1 &&
			subtle.ConstantTimeCompare([]byte(req.Pass), []byte(viewerPass)) == 1 {
			role = "viewer"
		}

		if role == "" {
			recordFailedLogin(clientIP)
			writeJSON(w, http.StatusUnauthorized, map[string]interface{}{"ok": false, "error": "invalid credentials"})
			return
		}

		// Clean expired sessions before creating new one
		if len(sessions) > maxSessions {
			cleanExpiredSessions()
		}

		token := genToken()
		sessionsMu.Lock()
		sessions[token] = sessionEntry{expiry: time.Now().Add(sessionTTL), role: role}
		sessionsMu.Unlock()

		http.SetCookie(w, &http.Cookie{
			Name:     sessionCookie,
			Value:    token,
			Path:     "/",
			HttpOnly: true,
			// Secure: true omitted — dashboard accessed via SSM port forwarding (localhost HTTP)
			SameSite: http.SameSiteStrictMode,
			MaxAge:   int(sessionTTL.Seconds()),
		})
		writeJSON(w, http.StatusOK, map[string]interface{}{"ok": true, "role": role})
	})

	// Auth middleware for API + dashboard page (any role)
	authRequired := func(next http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			ownerUser, ownerPass := getCredentials(s.Config)
			if ownerUser == "" || ownerPass == "" {
				writeJSON(w, http.StatusForbidden, map[string]interface{}{
					"ok":    false,
					"error": "dashboard credentials not configured",
				})
				return
			}
			if !validSession(r) {
				w.WriteHeader(http.StatusUnauthorized)
				_, _ = w.Write([]byte(`{"ok":false,"error":"unauthorized"}`))
				return
			}
			next(w, r)
		}
	}

	// Owner-only middleware — blocks viewers from write operations
	ownerRequired := func(next http.HandlerFunc) http.HandlerFunc {
		return authRequired(func(w http.ResponseWriter, r *http.Request) {
			if sessionRole(r) != "owner" {
				writeJSON(w, http.StatusForbidden, map[string]interface{}{
					"ok":    false,
					"error": "owner access required — viewers cannot modify tables",
				})
				return
			}
			next(w, r)
		})
	}

	// Dashboard page
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		// Serve index.html always — JS handles login UI on 401
		data, err := indexHTML.ReadFile("index.html")
		if err != nil {
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write(data)
	})

	// API: status
	mux.HandleFunc("/api/status", authRequired(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		s.RunningMu.Lock()
		tables := make([]map[string]interface{}, 0, len(*s.Running))
		for name, active := range *s.Running {
			parts := strings.SplitN(name, ".", 2)
			ks, tbl := "", name
			if len(parts) == 2 {
				ks = parts[0]
				tbl = parts[1]
			}
			tables = append(tables, map[string]interface{}{
				"name":     tbl,
				"keyspace": ks,
				"fqn":      name,
				"active":   active,
			})
		}
		s.RunningMu.Unlock()

		// Read connection gauges from prometheus
		scyllaConn := readGaugeValue("scylla2ch_scylla_connected")
		chConn := readGaugeValue("scylla2ch_clickhouse_connected")

		writeJSON(w, http.StatusOK, map[string]interface{}{
			"version":             s.Version,
			"uptime_seconds":      time.Since(s.StartTime).Seconds(),
			"tables":              tables,
			"scylla_connected":    scyllaConn == 1,
			"clickhouse_connected": chConn == 1,
			"role":                sessionRole(r),
			"config": map[string]interface{}{
				"auto_discover": s.Config.Tables.AutoDiscover,
				"auto_create":   s.Config.Tables.AutoCreate,
				"batch_size":    s.Config.Batch.Size,
				"batch_interval": s.Config.Batch.Interval.String(),
			},
		})
	}))

	// API: metrics
	mux.HandleFunc("/api/metrics", authRequired(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		families, err := prometheus.DefaultGatherer.Gather()
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]interface{}{"ok": false, "error": err.Error()})
			return
		}

		perTable := make(map[string]map[string]interface{})
		var totalConsumed, totalFlushed float64
		var consumersActive, tablesActive float64

		for _, mf := range families {
			name := mf.GetName()
			for _, m := range mf.GetMetric() {
				tbl := labelValue(m, "table")

				switch name {
				case "scylla2ch_rows_consumed_total":
					if tbl != "" {
						ensureTable(perTable, tbl)
						v := m.GetCounter().GetValue()
						perTable[tbl]["rows_consumed"] = v
						totalConsumed += v
					}
				case "scylla2ch_rows_flushed_total":
					if tbl != "" {
						ensureTable(perTable, tbl)
						v := m.GetCounter().GetValue()
						perTable[tbl]["rows_flushed"] = v
						totalFlushed += v
					}
				case "scylla2ch_rows_dropped_total":
					if tbl != "" {
						ensureTable(perTable, tbl)
						perTable[tbl]["rows_dropped"] = m.GetCounter().GetValue()
					}
				case "scylla2ch_buffer_rows":
					if tbl != "" {
						ensureTable(perTable, tbl)
						perTable[tbl]["buffer_size"] = m.GetGauge().GetValue()
					}
				case "scylla2ch_cdc_lag_seconds":
					if tbl != "" {
						ensureTable(perTable, tbl)
						perTable[tbl]["cdc_lag_seconds"] = m.GetGauge().GetValue()
					}
				case "scylla2ch_flush_duration_seconds":
					if tbl != "" {
						ensureTable(perTable, tbl)
						h := m.GetHistogram()
						perTable[tbl]["flush_latency_p50"] = quantileFromHistogram(h, 0.5)
						perTable[tbl]["flush_latency_p99"] = quantileFromHistogram(h, 0.99)
					}
				case "scylla2ch_flushes_total":
					if tbl != "" {
						ensureTable(perTable, tbl)
						status := labelValue(m, "status")
						if status == "ok" {
							perTable[tbl]["flushes_ok"] = m.GetCounter().GetValue()
						} else if status == "error" {
							perTable[tbl]["flushes_error"] = m.GetCounter().GetValue()
						}
					}
				case "scylla2ch_consumers_active":
					consumersActive = m.GetGauge().GetValue()
				case "scylla2ch_tables_active":
					tablesActive = m.GetGauge().GetValue()
				}
			}
		}

		writeJSON(w, http.StatusOK, map[string]interface{}{
			"tables": perTable,
			"totals": map[string]interface{}{
				"rows_consumed":    totalConsumed,
				"rows_flushed":     totalFlushed,
				"consumers_active": consumersActive,
				"tables_active":    tablesActive,
			},
		})
	}))

	// API: add table
	mux.HandleFunc("/api/tables", ownerRequired(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			handleAddTable(w, r, s)
		case http.MethodDelete:
			handleRemoveTable(w, r, s)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	}))

	return mux
}

func handleAddTable(w http.ResponseWriter, r *http.Request, s *State) {
	var req struct {
		Name    string `json:"name"`
		CHTable string `json:"ch_table"` // optional: ClickHouse target (fully-qualified like chat.pending_rooms)
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1024)).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{"ok": false, "error": "invalid JSON"})
		return
	}
	req.Name = strings.TrimSpace(req.Name)
	req.CHTable = strings.TrimSpace(req.CHTable)
	if req.Name == "" {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{"ok": false, "error": "name is required"})
		return
	}

	// Parse ScyllaDB input: accept "keyspace.table" or just "table"
	var scyllaKS, scyllaTable string
	if parts := strings.SplitN(req.Name, ".", 2); len(parts) == 2 {
		scyllaKS = parts[0]
		scyllaTable = parts[1]
	} else {
		scyllaTable = req.Name
	}

	// Parse ClickHouse input: accept "database.table" or just "table"
	// If fully-qualified (has dot), use as-is. If bare name, we'll prepend the mapped database.
	chTarget := req.CHTable
	chFullyQualified := false
	if chTarget != "" && strings.Contains(chTarget, ".") {
		chFullyQualified = true
	}
	if chTarget == "" {
		chTarget = scyllaTable // default: same table name
	}

	// The config stores just the ScyllaDB table name (without keyspace prefix)
	configName := scyllaTable

	// Check if already in config
	for _, t := range s.Config.Tables.Explicit {
		if t.Name == configName {
			writeJSON(w, http.StatusConflict, map[string]interface{}{"ok": false, "error": fmt.Sprintf("%s already in config", configName)})
			return
		}
	}

	// Pre-flight: verify CDC is enabled on ScyllaDB + ClickHouse table exists
	// system_schema.tables is cluster-wide — ANY session can query ANY keyspace's tables.
	// So we grab the first available session and use it for all CDC checks.
	var anySession *gocql.Session
	if s.SessionMap != nil {
		for _, sess := range s.SessionMap {
			if sess != nil {
				anySession = sess
				break
			}
		}
	}

	cdcFound := false
	chFound := false
	var cdcKeyspace, chFQN string

	if anySession == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]interface{}{
			"ok": false, "error": "no ScyllaDB session available — is ScyllaDB connected?",
		})
		return
	}

	// Determine which keyspaces to check for the CDC log table
	var keyspaces []string
	if scyllaKS != "" {
		// User specified keyspace explicitly (e.g. "chat.pending_rooms") — only check that one
		keyspaces = []string{scyllaKS}
	} else {
		// Bare table name — search all keyspaces on the cluster dynamically
		cdcLogTable := scyllaTable + "_scylla_cdc_log"
		iter := anySession.Query(
			"SELECT keyspace_name FROM system_schema.tables WHERE table_name = ? ALLOW FILTERING",
			cdcLogTable,
		).Iter()
		var ks string
		for iter.Scan(&ks) {
			keyspaces = append(keyspaces, ks)
		}
		_ = iter.Close()
		// If nothing found via dynamic scan, fall back to configured keyspaces
		if len(keyspaces) == 0 {
			keyspaces = s.Config.Scylla.AllKeyspaces()
		}
	}

	for _, ks := range keyspaces {
		// Check CDC: look for <table>_scylla_cdc_log in system_schema.tables
		cdcLogTable := scyllaTable + "_scylla_cdc_log"
		var count int
		if err := anySession.Query(
			"SELECT count(*) FROM system_schema.tables WHERE keyspace_name = ? AND table_name = ?",
			ks, cdcLogTable,
		).Scan(&count); err == nil && count > 0 {
			cdcFound = true
			cdcKeyspace = ks
		}

		if !cdcFound {
			continue
		}

		// Build ClickHouse fully-qualified name for existence check
		if chFullyQualified {
			chFQN = chTarget // already has database.table, use as-is
		} else {
			// Try CHDBMap first, fall back to keyspace name as ClickHouse database
			chDB := s.CHDBMap[ks]
			if chDB == "" {
				chDB = ks // e.g. frnddit keyspace → frnddit database
			}
			chFQN = fmt.Sprintf("%s.%s", chDB, chTarget)
		}

		if s.CHChecker != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			exists, err := s.CHChecker.TableExists(ctx, chFQN)
			cancel()
			if err != nil {
				s.Logger.Printf("dashboard: add table %s: ClickHouse check error: %v", req.Name, err)
				writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
					"ok": false, "error": fmt.Sprintf("ClickHouse check failed: %v", err),
				})
				return
			}
			if exists {
				chFound = true
			}
		}
		break
	}

	scyllaFQN := scyllaTable
	if cdcKeyspace != "" {
		scyllaFQN = cdcKeyspace + "." + scyllaTable
	}

	if !cdcFound {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"ok":    false,
			"error": fmt.Sprintf("CDC not enabled on ScyllaDB table '%s' — run: ALTER TABLE %s WITH cdc = {'enabled': true}", scyllaFQN, scyllaFQN),
		})
		return
	}

	if !chFound {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"ok":    false,
			"error": fmt.Sprintf("ClickHouse table '%s' does not exist — create it first", chFQN),
		})
		return
	}

	// Both CDC + ClickHouse table verified — save to config
	// If CH target differs from ScyllaDB table name, store as route
	routeTarget := ""
	if chFullyQualified || chTarget != configName {
		routeTarget = chTarget
	}
	if err := modifyConfigTables(s.ConfigFile, configName, true, routeTarget); err != nil {
		s.Logger.Printf("dashboard: add table %s: %v", configName, err)
		writeJSON(w, http.StatusInternalServerError, map[string]interface{}{"ok": false, "error": err.Error()})
		return
	}

	// Self-SIGHUP to reload — zero downtime, new reader starts immediately
	_ = syscall.Kill(syscall.Getpid(), syscall.SIGHUP)

	msg := fmt.Sprintf("added %s → %s — CDC verified, SIGHUP sent", scyllaFQN, chFQN)
	s.Logger.Printf("dashboard: %s", msg)
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"ok":      true,
		"message": msg,
	})
}

func handleRemoveTable(w http.ResponseWriter, r *http.Request, s *State) {
	name := strings.TrimSpace(r.URL.Query().Get("name"))
	if name == "" {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{"ok": false, "error": "name query param is required"})
		return
	}

	if err := modifyConfigTables(s.ConfigFile, name, false, ""); err != nil {
		s.Logger.Printf("dashboard: remove table %s: %v", name, err)
		writeJSON(w, http.StatusInternalServerError, map[string]interface{}{"ok": false, "error": err.Error()})
		return
	}

	s.Logger.Printf("dashboard: removed table %s from config (reader stops on next restart)", name)
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"ok":      true,
		"message": fmt.Sprintf("removed %s from config (reader stops on next restart)", name),
	})
}

// modifyConfigTables reads the config YAML, adds or removes a table from tables.explicit, writes back.
// routeTarget is the ClickHouse table name when it differs from the ScyllaDB table name (empty = same name).
func modifyConfigTables(cfgFile string, tableName string, add bool, routeTarget string) error {
	data, err := os.ReadFile(cfgFile)
	if err != nil {
		return fmt.Errorf("read config: %w", err)
	}

	// Parse as generic yaml node to preserve comments and structure
	var doc yaml.Node
	if err := yaml.Unmarshal(data, &doc); err != nil {
		return fmt.Errorf("parse config: %w", err)
	}

	// Navigate: doc.Content[0] is the root mapping
	if doc.Kind != yaml.DocumentNode || len(doc.Content) == 0 {
		return fmt.Errorf("unexpected yaml structure")
	}
	root := doc.Content[0]
	if root.Kind != yaml.MappingNode {
		return fmt.Errorf("root is not a mapping")
	}

	// Find "tables" key
	var tablesNode *yaml.Node
	for i := 0; i < len(root.Content)-1; i += 2 {
		if root.Content[i].Value == "tables" {
			tablesNode = root.Content[i+1]
			break
		}
	}
	if tablesNode == nil || tablesNode.Kind != yaml.MappingNode {
		return fmt.Errorf("tables section not found in config")
	}

	// Find "explicit" key
	var explicitNode *yaml.Node
	for i := 0; i < len(tablesNode.Content)-1; i += 2 {
		if tablesNode.Content[i].Value == "explicit" {
			explicitNode = tablesNode.Content[i+1]
			break
		}
	}
	if explicitNode == nil || explicitNode.Kind != yaml.SequenceNode {
		return fmt.Errorf("tables.explicit section not found")
	}

	if add {
		// Add new entry
		entryContent := []*yaml.Node{
			{Kind: yaml.ScalarNode, Tag: "!!str", Value: "name"},
			{Kind: yaml.ScalarNode, Tag: "!!str", Value: tableName, Style: yaml.DoubleQuotedStyle},
		}
		// If ClickHouse table name differs, add routes
		if routeTarget != "" {
			routeEntry := &yaml.Node{
				Kind: yaml.MappingNode,
				Tag:  "!!map",
				Content: []*yaml.Node{
					{Kind: yaml.ScalarNode, Tag: "!!str", Value: "target"},
					{Kind: yaml.ScalarNode, Tag: "!!str", Value: routeTarget, Style: yaml.DoubleQuotedStyle},
				},
			}
			routesSeq := &yaml.Node{
				Kind:    yaml.SequenceNode,
				Tag:     "!!seq",
				Content: []*yaml.Node{routeEntry},
			}
			entryContent = append(entryContent,
				&yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: "routes"},
				routesSeq,
			)
		}
		entry := &yaml.Node{
			Kind:    yaml.MappingNode,
			Tag:     "!!map",
			Content: entryContent,
		}
		explicitNode.Content = append(explicitNode.Content, entry)
	} else {
		// Remove entry
		var newContent []*yaml.Node
		found := false
		for _, item := range explicitNode.Content {
			if item.Kind == yaml.MappingNode {
				for j := 0; j < len(item.Content)-1; j += 2 {
					if item.Content[j].Value == "name" && item.Content[j+1].Value == tableName {
						found = true
						break
					}
				}
			}
			if !found {
				newContent = append(newContent, item)
			}
			found = false
		}
		// Check if actually removed something
		if len(newContent) == len(explicitNode.Content) {
			return fmt.Errorf("table %s not found in config", tableName)
		}
		explicitNode.Content = newContent
	}

	// Write back
	out, err := yaml.Marshal(&doc)
	if err != nil {
		return fmt.Errorf("marshal config: %w", err)
	}
	if err := os.WriteFile(cfgFile, out, 0644); err != nil {
		return fmt.Errorf("write config: %w", err)
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func labelValue(m *dto.Metric, name string) string {
	for _, lp := range m.GetLabel() {
		if lp.GetName() == name {
			return lp.GetValue()
		}
	}
	return ""
}

func ensureTable(m map[string]map[string]interface{}, tbl string) {
	if _, ok := m[tbl]; !ok {
		m[tbl] = make(map[string]interface{})
	}
}

func readGaugeValue(metricName string) float64 {
	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		return 0
	}
	for _, mf := range families {
		if mf.GetName() == metricName {
			for _, m := range mf.GetMetric() {
				return m.GetGauge().GetValue()
			}
		}
	}
	return 0
}

// quantileFromHistogram estimates a quantile from a histogram's buckets.
func quantileFromHistogram(h *dto.Histogram, q float64) float64 {
	if h == nil {
		return 0
	}
	target := q * float64(h.GetSampleCount())
	if target == 0 {
		return 0
	}
	var prevCount uint64
	var prevBound float64
	for _, b := range h.GetBucket() {
		if float64(b.GetCumulativeCount()) >= target {
			// Linear interpolation within bucket
			count := b.GetCumulativeCount() - prevCount
			if count == 0 {
				return b.GetUpperBound()
			}
			fraction := (target - float64(prevCount)) / float64(count)
			return prevBound + fraction*(b.GetUpperBound()-prevBound)
		}
		prevCount = b.GetCumulativeCount()
		prevBound = b.GetUpperBound()
	}
	// Beyond last bucket — return sum/count as fallback
	if h.GetSampleCount() > 0 {
		return h.GetSampleSum() / float64(h.GetSampleCount())
	}
	return 0
}
