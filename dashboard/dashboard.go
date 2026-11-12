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
	"regexp"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/gocql/gocql"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"gopkg.in/yaml.v3"

	"scylla2ch/config"
	"scylla2ch/converter"
)

// FIX M4: Table name validation — only allow safe identifier characters.
// Prevents YAML injection or filesystem issues from malicious table names.
var validTableName = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

//go:embed index.html
var indexHTML embed.FS

// CHTableChecker abstracts the ClickHouse writer so the dashboard package
// doesn't import the writer package directly.
type CHTableChecker interface {
	TableExists(ctx context.Context, table string) (bool, error)
	TableColumns(ctx context.Context, table string) ([]config.CHColumn, error)
	SetTableBatch(table string, size int, interval time.Duration)
	GetTableBatchConfig(table string) (int, time.Duration)
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

	// For pre-flight validation when adding tables.
	// SessionMap and CHDBMap are written by SIGHUP reload goroutine and read by HTTP handlers.
	// SessionMu protects both maps from concurrent access.
	SessionMap map[string]*gocql.Session // keyspace → ScyllaDB session
	CHDBMap    map[string]string         // keyspace → ClickHouse database
	SessionMu  *sync.RWMutex            // protects SessionMap and CHDBMap
	CHChecker  CHTableChecker            // checks ClickHouse table existence

	// StopTable cancels the CDC reader for a table and removes it from Running.
	// Set by cmd/run.go — nil means stop-on-remove is not supported (falls back to restart).
	StopTable func(fqn string) bool
}

// session store — token → {expiry, role}
type sessionEntry struct {
	expiry time.Time
	role   string // "owner" or "viewer"
	user   string // username for clearing old sessions on re-login
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

// configFileMu serializes writes to the config YAML file.
// modifyConfigTables and modifyConfigBatch both read-modify-write the same file;
// without this mutex, concurrent dashboard requests can corrupt the YAML.
var configFileMu sync.Mutex

const sessionTTLOwner = 7 * 24 * time.Hour  // admin sessions last 7 days
const sessionTTLViewer = 24 * time.Hour      // viewer sessions last 24h
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

		// Use RemoteAddr directly — do NOT trust X-Forwarded-For (spoofable, bypasses rate limit).
		// Dashboard is accessed via SSM port forwarding or direct connection, not behind a proxy.
		clientIP := r.RemoteAddr

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

		// Clean expired sessions + clear old sessions for the same user (re-login)
		sessionsMu.Lock()
		for k, entry := range sessions {
			if entry.user == req.User || time.Now().After(entry.expiry) {
				delete(sessions, k)
			}
		}
		sessionsMu.Unlock()

		ttl := sessionTTLViewer
		if role == "owner" {
			ttl = sessionTTLOwner
		}

		token := genToken()
		sessionsMu.Lock()
		sessions[token] = sessionEntry{expiry: time.Now().Add(ttl), role: role, user: req.User}
		sessionsMu.Unlock()

		http.SetCookie(w, &http.Cookie{
			Name:     sessionCookie,
			Value:    token,
			Path:     "/",
			HttpOnly: true,
			// Secure: true omitted — dashboard accessed via SSM port forwarding (localhost HTTP)
			SameSite: http.SameSiteStrictMode,
			MaxAge:   int(ttl.Seconds()),
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
			entry := map[string]interface{}{
				"name":     tbl,
				"keyspace": ks,
				"fqn":      name,
				"active":   active,
			}
			// Include per-table batch config if set
			if s.CHChecker != nil {
				// Build CH qualified name to look up batch config
				chDB := ks
				if s.SessionMu != nil {
					s.SessionMu.RLock()
					if mapped := s.CHDBMap[ks]; mapped != "" {
						chDB = mapped
					}
					s.SessionMu.RUnlock()
				}
				chFQN := chDB + "." + tbl
				bs, bi := s.CHChecker.GetTableBatchConfig(chFQN)
				if bs > 0 {
					entry["batch_size"] = bs
				}
				if bi > 0 {
					entry["batch_interval"] = bi.String()
				}
			}
			// Include per-table change_age_limit if set
			if tc := s.Config.GetTableConfig(tbl); tc != nil && tc.ChangeAgeLimit > 0 {
				entry["change_age_limit"] = tc.ChangeAgeLimit.String()
			}
			tables = append(tables, entry)
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
				case "scylla2ch_rows_dlq_total":
				if tbl != "" {
					ensureTable(perTable, tbl)
					perTable[tbl]["rows_dlq"] = m.GetCounter().GetValue()
				}
			case "scylla2ch_retries_total":
				if tbl != "" {
					ensureTable(perTable, tbl)
					perTable[tbl]["retries"] = m.GetCounter().GetValue()
				}
			case "scylla2ch_reader_errors_total":
				if tbl != "" {
					ensureTable(perTable, tbl)
					perTable[tbl]["reader_errors"] = m.GetCounter().GetValue()
				}
			case "scylla2ch_deletes_skipped_total":
				if tbl != "" {
					ensureTable(perTable, tbl)
					perTable[tbl]["deletes_skipped"] = m.GetCounter().GetValue()
				}
			case "scylla2ch_last_consumed_timestamp_ms":
					if tbl != "" {
						ensureTable(perTable, tbl)
						perTable[tbl]["last_consumed_ms"] = m.GetGauge().GetValue()
					}
				case "scylla2ch_last_flushed_timestamp_ms":
					if tbl != "" {
						ensureTable(perTable, tbl)
						perTable[tbl]["last_flushed_ms"] = m.GetGauge().GetValue()
					}
				case "scylla2ch_errors_total":
					component := labelValue(m, "component")
					severity := labelValue(m, "severity")
					if component != "" {
						key := "errors_" + component + "_" + severity
						ensureTable(perTable, "_global")
						perTable["_global"][key] = m.GetCounter().GetValue()
					}
				case "scylla2ch_consumers_active":
					consumersActive = m.GetGauge().GetValue()
				case "scylla2ch_tables_active":
					tablesActive = m.GetGauge().GetValue()
				case "scylla2ch_goroutines":
					ensureTable(perTable, "_global")
					perTable["_global"]["goroutines"] = m.GetGauge().GetValue()
				case "scylla2ch_heap_alloc_bytes":
					ensureTable(perTable, "_global")
					perTable["_global"]["heap_alloc_bytes"] = m.GetGauge().GetValue()
				}
			}
		}

		// Extract global errors and runtime metrics into separate field
		globalMeta := perTable["_global"]
		delete(perTable, "_global")
		var goroutines, heapBytes interface{}
		if globalMeta != nil {
			goroutines = globalMeta["goroutines"]
			heapBytes = globalMeta["heap_alloc_bytes"]
			delete(globalMeta, "goroutines")
			delete(globalMeta, "heap_alloc_bytes")
		}

		writeJSON(w, http.StatusOK, map[string]interface{}{
			"tables": perTable,
			"errors": globalMeta,
			"totals": map[string]interface{}{
				"rows_consumed":    totalConsumed,
				"rows_flushed":     totalFlushed,
				"consumers_active": consumersActive,
				"tables_active":    tablesActive,
				"goroutines":       goroutines,
				"heap_alloc_bytes": heapBytes,
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

	// API: schema check — compare ScyllaDB vs ClickHouse column types
	mux.HandleFunc("/api/schema-check", authRequired(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		scyllaParam := strings.TrimSpace(r.URL.Query().Get("scylla"))  // e.g. "chat.messages"
		chParam := strings.TrimSpace(r.URL.Query().Get("ch"))          // e.g. "chat.messages"
		if scyllaParam == "" || chParam == "" {
			writeJSON(w, http.StatusBadRequest, map[string]interface{}{
				"ok": false, "error": "scylla and ch query params required (e.g. ?scylla=chat.messages&ch=chat.messages)",
			})
			return
		}

		// Parse ScyllaDB keyspace.table
		parts := strings.SplitN(scyllaParam, ".", 2)
		if len(parts) != 2 {
			writeJSON(w, http.StatusBadRequest, map[string]interface{}{
				"ok": false, "error": "scylla param must be keyspace.table",
			})
			return
		}
		ks, tbl := parts[0], parts[1]

		var anySession *gocql.Session
		if s.SessionMu != nil {
			s.SessionMu.RLock()
		}
		if s.SessionMap != nil {
			for _, sess := range s.SessionMap {
				if sess != nil {
					anySession = sess
					break
				}
			}
		}
		if s.SessionMu != nil {
			s.SessionMu.RUnlock()
		}
		if anySession == nil || s.CHChecker == nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]interface{}{
				"ok": false, "error": "no database sessions available",
			})
			return
		}

		result := schemaCheckDetail(anySession, ks, tbl, chParam, s.CHChecker, s.Logger)
		writeJSON(w, http.StatusOK, result)
	}))

	// API: update config (batch size, interval) — owner only
	mux.HandleFunc("/api/config", ownerRequired(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPatch {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			BatchSize     *int    `json:"batch_size"`
			BatchInterval *string `json:"batch_interval"`
		}
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1024)).Decode(&req); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]interface{}{"ok": false, "error": "invalid JSON"})
			return
		}

		changed := false
		if req.BatchSize != nil {
			if *req.BatchSize < 100 || *req.BatchSize > 100000 {
				writeJSON(w, http.StatusBadRequest, map[string]interface{}{"ok": false, "error": "batch_size must be 100-100000"})
				return
			}
			s.Config.Batch.Size = *req.BatchSize
			changed = true
		}
		if req.BatchInterval != nil {
			d, err := time.ParseDuration(*req.BatchInterval)
			if err != nil || d < 500*time.Millisecond || d > 60*time.Second {
				writeJSON(w, http.StatusBadRequest, map[string]interface{}{"ok": false, "error": "batch_interval must be 500ms-60s"})
				return
			}
			s.Config.Batch.Interval = d
			changed = true
		}

		if !changed {
			writeJSON(w, http.StatusBadRequest, map[string]interface{}{"ok": false, "error": "nothing to update"})
			return
		}

		if err := modifyConfigBatch(s.ConfigFile, req.BatchSize, req.BatchInterval); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]interface{}{"ok": false, "error": err.Error()})
			return
		}

		// SIGHUP to reload config (writer picks up new batch settings)
		_ = syscall.Kill(syscall.Getpid(), syscall.SIGHUP)

		writeJSON(w, http.StatusOK, map[string]interface{}{
			"ok":      true,
			"message": fmt.Sprintf("config updated: batch_size=%d interval=%s", s.Config.Batch.Size, s.Config.Batch.Interval),
		})
	}))

	return mux
}

func handleAddTable(w http.ResponseWriter, r *http.Request, s *State) {
	var req struct {
		Name           string `json:"name"`
		CHTable        string `json:"ch_table"`        // optional: ClickHouse target (fully-qualified like chat.pending_rooms)
		BatchSize      int    `json:"batch_size"`       // optional: per-table batch size (0 = global default)
		BatchInterval  string `json:"batch_interval"`   // optional: per-table flush interval e.g. "5s" (empty = global default)
		ChangeAgeLimit string `json:"change_age_limit"` // optional: how far back to read on first start e.g. "10s", "1h", "8h" (empty = global default)
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

	// FIX M4: Validate table name characters — prevent YAML/path injection.
	// Accept "keyspace.table" or just "table", each part must be a valid identifier.
	for _, part := range strings.SplitN(req.Name, ".", 2) {
		if !validTableName.MatchString(part) {
			writeJSON(w, http.StatusBadRequest, map[string]interface{}{"ok": false, "error": fmt.Sprintf("invalid table name %q — only letters, digits, underscores allowed", part)})
			return
		}
	}

	// Parse per-table batch config (like Kafka per-topic consumer config)
	var batchInterval time.Duration
	if req.BatchInterval != "" {
		var parseErr error
		batchInterval, parseErr = time.ParseDuration(req.BatchInterval)
		if parseErr != nil {
			writeJSON(w, http.StatusBadRequest, map[string]interface{}{"ok": false, "error": fmt.Sprintf("invalid batch_interval: %v (use e.g. '5s', '500ms')", parseErr)})
			return
		}
		if batchInterval < 100*time.Millisecond {
			writeJSON(w, http.StatusBadRequest, map[string]interface{}{"ok": false, "error": "batch_interval must be >= 100ms"})
			return
		}
	}
	if req.BatchSize < 0 {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{"ok": false, "error": "batch_size must be >= 0"})
		return
	}

	// Parse per-table CDC age limit (how far back to read on first start).
	// Default "10s" = start from ~now (covers confidence window).
	// Without a per-table override, the global default (8h) would replay old history.
	changeAgeLimit := 10 * time.Second
	if req.ChangeAgeLimit != "" {
		var parseErr error
		changeAgeLimit, parseErr = time.ParseDuration(req.ChangeAgeLimit)
		if parseErr != nil {
			writeJSON(w, http.StatusBadRequest, map[string]interface{}{"ok": false, "error": fmt.Sprintf("invalid change_age_limit: %v (use e.g. '10s', '1h', '8h')", parseErr)})
			return
		}
		if changeAgeLimit < 0 {
			writeJSON(w, http.StatusBadRequest, map[string]interface{}{"ok": false, "error": "change_age_limit must be >= 0"})
			return
		}
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
	if s.SessionMu != nil {
		s.SessionMu.RLock()
	}
	if s.SessionMap != nil {
		for _, sess := range s.SessionMap {
			if sess != nil {
				anySession = sess
				break
			}
		}
	}
	if s.SessionMu != nil {
		s.SessionMu.RUnlock()
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
		// Bare table name — get all keyspaces, then check each with proper partition-key query.
		// No ALLOW FILTERING — system_schema.keyspaces is tiny and this is a full-PK scan.
		iter := anySession.Query("SELECT keyspace_name FROM system_schema.keyspaces").Iter()
		var ks string
		for iter.Scan(&ks) {
			// Skip system keyspaces
			if strings.HasPrefix(ks, "system") {
				continue
			}
			keyspaces = append(keyspaces, ks)
		}
		_ = iter.Close()
		// If nothing found, fall back to configured keyspaces
		if len(keyspaces) == 0 {
			keyspaces = s.Config.Scylla.AllKeyspaces()
		}
	}

	var cdcCheckErr error
	s.Logger.Printf("dashboard: CDC check: scyllaTable=%q scyllaKS=%q keyspaces=%v", scyllaTable, scyllaKS, keyspaces)
	for _, ks := range keyspaces {
		// Check CDC using two methods:
		// 1. Primary: query base table's cdc config from system_schema.scylla_tables (source of truth)
		// 2. Fallback: check if <table>_scylla_cdc_log exists in system_schema.tables
		//    (works on all ScyllaDB versions, but can false-positive from stale schema metadata)

		// Method 1: scylla_tables cdc config
		var cdcConfig map[string]string
		if err := anySession.Query(
			"SELECT cdc FROM system_schema.scylla_tables WHERE keyspace_name = ? AND table_name = ?",
			ks, scyllaTable,
		).Scan(&cdcConfig); err != nil {
			s.Logger.Printf("dashboard: CDC config check for %s.%s failed (trying fallback): %v", ks, scyllaTable, err)
		} else {
			s.Logger.Printf("dashboard: CDC config %s.%s cdc=%v", ks, scyllaTable, cdcConfig)
			if cdcConfig["enabled"] == "true" {
				cdcFound = true
				cdcKeyspace = ks
			}
		}

		// Method 2: fallback — check CDC log table exists
		if !cdcFound {
			cdcLogTable := scyllaTable + "_scylla_cdc_log"
			var count int
			if err := anySession.Query(
				"SELECT count(*) FROM system_schema.tables WHERE keyspace_name = ? AND table_name = ?",
				ks, cdcLogTable,
			).Scan(&count); err != nil {
				s.Logger.Printf("dashboard: CDC fallback check for %s.%s failed: %v", ks, cdcLogTable, err)
				cdcCheckErr = err
			} else {
				s.Logger.Printf("dashboard: CDC fallback %s.%s count=%d", ks, cdcLogTable, count)
				if count > 0 {
					cdcFound = true
					cdcKeyspace = ks
				}
			}
		}

		if !cdcFound {
			continue
		}

		// Build ClickHouse fully-qualified name for existence check
		if chFullyQualified {
			chFQN = chTarget // already has database.table, use as-is
		} else {
			// Try CHDBMap first, fall back to keyspace name as ClickHouse database
			if s.SessionMu != nil {
				s.SessionMu.RLock()
			}
			chDB := s.CHDBMap[ks]
			if s.SessionMu != nil {
				s.SessionMu.RUnlock()
			}
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
	} else if scyllaKS != "" {
		scyllaFQN = scyllaKS + "." + scyllaTable
	}

	if !cdcFound {
		errMsg := fmt.Sprintf("CDC not enabled on ScyllaDB table '%s' — run: ALTER TABLE %s WITH cdc = {'enabled': true}", scyllaFQN, scyllaFQN)
		if cdcCheckErr != nil {
			errMsg = fmt.Sprintf("ScyllaDB query error checking CDC for '%s': %v", scyllaFQN, cdcCheckErr)
		}
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"ok":    false,
			"error": errMsg,
		})
		return
	}

	if !chFound {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"ok":    false,
			"error": fmt.Sprintf("ClickHouse table '%s' does not exist — create it first using the Create Table button", chFQN),
		})
		return
	}

	// Pre-flight: compare ScyllaDB vs ClickHouse schema for type mismatches
	var schemaWarnings []string
	var schemaErrors []string
	if anySession != nil && s.CHChecker != nil {
		schemaWarnings, schemaErrors = compareSchema(anySession, cdcKeyspace, scyllaTable, chFQN, s.CHChecker, s.Logger)
	}

	// Block save if any column types don't match — prevents poison rows at runtime
	if len(schemaErrors) > 0 {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"ok":     false,
			"error":  fmt.Sprintf("Schema mismatch: %d column type(s) don't match between ScyllaDB and ClickHouse — fix the ClickHouse table first", len(schemaErrors)),
			"errors": schemaErrors,
		})
		return
	}

	// Reset checkpoint so the reader starts fresh with change_age_limit.
	// Two writes needed:
	//   1. Clear per-stream progress rows for this table (scylla-cdc-go stores as "keyspace.table")
	//   2. Update the application start time to now-changeAgeLimit
	//      (the global current_generation row would otherwise make readFrom = old generation time)
	// Both together make the reader start from the configured time.
	// Existing running tables are unaffected — their per-stream progress is current and wins.
	if anySession != nil {
		progressTable := fmt.Sprintf("%s.%s", cdcKeyspace, config.ProgressTable)
		progressTableName := cdcKeyspace + "." + scyllaTable
		clearTableCheckpoint(anySession, progressTable, progressTableName, s.Logger)
	}

	// Both CDC + ClickHouse table verified — save to config
	// If CH target differs from ScyllaDB table name, store as route
	routeTarget := ""
	if chFullyQualified || chTarget != configName {
		routeTarget = chTarget
	}
	if err := modifyConfigTables(s.ConfigFile, configName, true, routeTarget, cdcKeyspace, req.BatchSize, batchInterval, changeAgeLimit); err != nil {
		s.Logger.Printf("dashboard: add table %s: %v", configName, err)
		writeJSON(w, http.StatusInternalServerError, map[string]interface{}{"ok": false, "error": err.Error()})
		return
	}

	// Sync in-memory config so duplicate check stays accurate
	s.Config.Tables.Explicit = append(s.Config.Tables.Explicit, config.TableConfig{
		Name:           configName,
		Keyspace:       cdcKeyspace,
		BatchSize:      req.BatchSize,
		BatchInterval:  batchInterval,
		ChangeAgeLimit: changeAgeLimit,
	})

	// Set per-table batch config on the writer (takes effect when writer goroutine starts)
	if s.CHChecker != nil && (req.BatchSize > 0 || batchInterval > 0) {
		s.CHChecker.SetTableBatch(chFQN, req.BatchSize, batchInterval)
	}

	// Self-SIGHUP to reload — zero downtime, new reader starts immediately
	_ = syscall.Kill(syscall.Getpid(), syscall.SIGHUP)

	msg := fmt.Sprintf("added %s → %s — CDC verified, SIGHUP sent", scyllaFQN, chFQN)
	if len(schemaWarnings) > 0 {
		msg += fmt.Sprintf(" ⚠ %d schema warning(s)", len(schemaWarnings))
	}
	s.Logger.Printf("dashboard: %s", msg)
	resp := map[string]interface{}{
		"ok":      true,
		"message": msg,
	}
	if len(schemaWarnings) > 0 {
		resp["warnings"] = schemaWarnings
	}
	writeJSON(w, http.StatusOK, resp)
}

func handleRemoveTable(w http.ResponseWriter, r *http.Request, s *State) {
	name := strings.TrimSpace(r.URL.Query().Get("name"))
	if name == "" {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{"ok": false, "error": "name query param is required"})
		return
	}

	if err := modifyConfigTables(s.ConfigFile, name, false, "", "", 0, 0, 0); err != nil {
		s.Logger.Printf("dashboard: remove table %s: %v", name, err)
		writeJSON(w, http.StatusInternalServerError, map[string]interface{}{"ok": false, "error": err.Error()})
		return
	}

	// Sync in-memory config so duplicate check stays accurate
	newExplicit := make([]config.TableConfig, 0, len(s.Config.Tables.Explicit))
	for _, t := range s.Config.Tables.Explicit {
		if t.Name != name {
			newExplicit = append(newExplicit, t)
		}
	}
	s.Config.Tables.Explicit = newExplicit

	// Stop the CDC reader immediately (no restart needed)
	stopped := false
	if s.StopTable != nil {
		// Find the FQN (keyspace.table) from the running map
		s.RunningMu.Lock()
		var fqn string
		for k := range *s.Running {
			parts := strings.SplitN(k, ".", 2)
			if len(parts) == 2 && parts[1] == name {
				fqn = k
				break
			}
		}
		s.RunningMu.Unlock()

		if fqn != "" {
			stopped = s.StopTable(fqn)
		}
	}

	msg := fmt.Sprintf("removed %s from config", name)
	if stopped {
		msg += " — reader stopped"
	} else {
		msg += " — reader stops on next restart"
	}
	s.Logger.Printf("dashboard: %s", msg)
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"ok":      true,
		"message": msg,
	})
}

// modifyConfigTables reads the config YAML, adds or removes a table from tables.explicit, writes back.
// routeTarget is the ClickHouse table name when it differs from the ScyllaDB table name (empty = same name).
// keyspace is the ScyllaDB keyspace for this table (stored per-table to avoid cross-product).
func modifyConfigTables(cfgFile string, tableName string, add bool, routeTarget string, keyspace string, batchSize int, batchInterval time.Duration, changeAgeLimit time.Duration) error {
	configFileMu.Lock()
	defer configFileMu.Unlock()

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

	// Auto-add keyspace to scylla.keyspaces if not already present
	if add && keyspace != "" {
		addKeyspaceToConfig(root, keyspace)
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
		// Add new entry with per-table keyspace
		entryContent := []*yaml.Node{
			{Kind: yaml.ScalarNode, Tag: "!!str", Value: "name"},
			{Kind: yaml.ScalarNode, Tag: "!!str", Value: tableName, Style: yaml.DoubleQuotedStyle},
		}
		// Store keyspace per table — this avoids cross-product with all keyspaces
		if keyspace != "" {
			entryContent = append(entryContent,
				&yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: "keyspace"},
				&yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: keyspace, Style: yaml.DoubleQuotedStyle},
			)
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
		// Per-table batch config (like Kafka per-topic consumer config)
		if batchSize > 0 {
			entryContent = append(entryContent,
				&yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: "batch_size"},
				&yaml.Node{Kind: yaml.ScalarNode, Tag: "!!int", Value: fmt.Sprintf("%d", batchSize)},
			)
		}
		if batchInterval > 0 {
			entryContent = append(entryContent,
				&yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: "batch_interval"},
				&yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: batchInterval.String()},
			)
		}
		if changeAgeLimit > 0 {
			entryContent = append(entryContent,
				&yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: "change_age_limit"},
				&yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: changeAgeLimit.String()},
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
			// Build list of what's in config for debugging
			var names []string
			for _, item := range explicitNode.Content {
				if item.Kind == yaml.MappingNode {
					for j := 0; j < len(item.Content)-1; j += 2 {
						if item.Content[j].Value == "name" {
							names = append(names, item.Content[j+1].Value)
						}
					}
				}
			}
			return fmt.Errorf("table %q not found in config (explicit has %d entries: %v) — config may have been overwritten by a deploy", tableName, len(explicitNode.Content), names)
		}
		explicitNode.Content = newContent
	}

	// FIX M6: Atomic config write via temp file + rename.
	// Prevents SIGHUP from reading a partially-written file.
	out, err := yaml.Marshal(&doc)
	if err != nil {
		return fmt.Errorf("marshal config: %w", err)
	}
	tmpFile := cfgFile + ".tmp"
	if err := os.WriteFile(tmpFile, out, 0644); err != nil {
		return fmt.Errorf("write temp config: %w", err)
	}
	if err := os.Rename(tmpFile, cfgFile); err != nil {
		os.Remove(tmpFile) // cleanup on rename failure
		return fmt.Errorf("rename config: %w", err)
	}
	return nil
}

// addKeyspaceToConfig adds a keyspace to scylla.keyspaces in the YAML node tree if not already present.
func addKeyspaceToConfig(root *yaml.Node, keyspace string) {
	// Find "scylla" key
	var scyllaNode *yaml.Node
	for i := 0; i < len(root.Content)-1; i += 2 {
		if root.Content[i].Value == "scylla" {
			scyllaNode = root.Content[i+1]
			break
		}
	}
	if scyllaNode == nil || scyllaNode.Kind != yaml.MappingNode {
		return
	}

	// Find "keyspaces" key
	var ksNode *yaml.Node
	for i := 0; i < len(scyllaNode.Content)-1; i += 2 {
		if scyllaNode.Content[i].Value == "keyspaces" {
			ksNode = scyllaNode.Content[i+1]
			break
		}
	}
	if ksNode == nil || ksNode.Kind != yaml.SequenceNode {
		return
	}

	// Check if keyspace already in the list
	for _, item := range ksNode.Content {
		if item.Value == keyspace {
			return // already present
		}
	}

	// Add the new keyspace
	ksNode.Content = append(ksNode.Content, &yaml.Node{
		Kind:  yaml.ScalarNode,
		Tag:   "!!str",
		Value: keyspace,
		Style: yaml.DoubleQuotedStyle,
	})
}

// modifyConfigBatch updates batch.size and/or batch.interval in the config YAML file.
func modifyConfigBatch(cfgFile string, batchSize *int, batchInterval *string) error {
	configFileMu.Lock()
	defer configFileMu.Unlock()

	data, err := os.ReadFile(cfgFile)
	if err != nil {
		return fmt.Errorf("read config: %w", err)
	}

	var doc yaml.Node
	if err := yaml.Unmarshal(data, &doc); err != nil {
		return fmt.Errorf("parse config: %w", err)
	}

	if doc.Kind != yaml.DocumentNode || len(doc.Content) == 0 {
		return fmt.Errorf("unexpected yaml structure")
	}
	root := doc.Content[0]
	if root.Kind != yaml.MappingNode {
		return fmt.Errorf("root is not a mapping")
	}

	// Find or create "batch" key
	var batchNode *yaml.Node
	for i := 0; i < len(root.Content)-1; i += 2 {
		if root.Content[i].Value == "batch" {
			batchNode = root.Content[i+1]
			break
		}
	}
	if batchNode == nil {
		// Add batch section
		root.Content = append(root.Content,
			&yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: "batch"},
			&yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"},
		)
		batchNode = root.Content[len(root.Content)-1]
	}

	setYAMLField := func(node *yaml.Node, key, value string) {
		for i := 0; i < len(node.Content)-1; i += 2 {
			if node.Content[i].Value == key {
				node.Content[i+1].Value = value
				return
			}
		}
		node.Content = append(node.Content,
			&yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: key},
			&yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: value},
		)
	}

	if batchSize != nil {
		setYAMLField(batchNode, "size", fmt.Sprintf("%d", *batchSize))
	}
	if batchInterval != nil {
		setYAMLField(batchNode, "interval", *batchInterval)
	}

	// FIX M6: Atomic config write (same as modifyConfigTables)
	out, err := yaml.Marshal(&doc)
	if err != nil {
		return fmt.Errorf("marshal config: %w", err)
	}
	tmpFile := cfgFile + ".tmp"
	if err := os.WriteFile(tmpFile, out, 0644); err != nil {
		return fmt.Errorf("write temp config: %w", err)
	}
	if err := os.Rename(tmpFile, cfgFile); err != nil {
		os.Remove(tmpFile)
		return fmt.Errorf("rename config: %w", err)
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

// compareSchema returns warnings (informational) and errors (type mismatches that block save).
// Called during add-table pre-flight. Type mismatches are errors because they cause poison rows.
func compareSchema(scyllaSession *gocql.Session, keyspace, table, chFQN string, checker CHTableChecker, logger *log.Logger) (warnings []string, errors []string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Get ScyllaDB columns (from the base table, not the CDC log)
	scyllaCols := make(map[string]string) // name → CQL type
	iter := scyllaSession.Query(
		"SELECT column_name, type FROM system_schema.columns WHERE keyspace_name = ? AND table_name = ?",
		keyspace, table,
	).WithContext(ctx).Iter()
	var colName, colType string
	for iter.Scan(&colName, &colType) {
		scyllaCols[colName] = colType
	}
	if err := iter.Close(); err != nil {
		logger.Printf("dashboard schema-check: ScyllaDB columns query error: %v", err)
		return nil, nil
	}

	// Get ClickHouse columns
	chCols, err := checker.TableColumns(ctx, chFQN)
	if err != nil {
		logger.Printf("dashboard schema-check: ClickHouse columns query error: %v", err)
		return nil, nil
	}

	chColMap := make(map[string]string, len(chCols))
	for _, c := range chCols {
		chColMap[c.Name] = c.Type
	}

	// Check each ScyllaDB column against ClickHouse
	for scyllaCol, cqlType := range scyllaCols {
		chType, exists := chColMap[scyllaCol]
		if !exists {
			// Column missing in ClickHouse — this is OK (CDC delta mode), just note it
			warnings = append(warnings, fmt.Sprintf("column '%s' (%s) exists in ScyllaDB but not in ClickHouse — will be skipped", scyllaCol, cqlType))
			continue
		}
		// Compare expected type vs actual — mismatch = error (blocks save, prevents poison rows)
		expectedCH := converter.CQLToClickHouseType(cqlType, false) // assume non-PK for Nullable wrapping
		if !typeCompatible(expectedCH, chType) {
			errors = append(errors, fmt.Sprintf("type mismatch: column '%s' — ScyllaDB %s → expected CH %s, got CH %s", scyllaCol, cqlType, expectedCH, chType))
		}
	}

	// _cdc_ts is REQUIRED — it's the ReplacingMergeTree version column.
	// Without it, dedup doesn't work and every CDC re-delivery creates a duplicate row.
	if cdcTsType, hasCdcTs := chColMap["_cdc_ts"]; !hasCdcTs {
		errors = append(errors, "missing required column '_cdc_ts' (DateTime64) — needed for ReplacingMergeTree dedup. Add it: ALTER TABLE ... ADD COLUMN `_cdc_ts` DateTime64(6) DEFAULT now64()")
	} else if !strings.Contains(cdcTsType, "DateTime64") {
		errors = append(errors, fmt.Sprintf("wrong type for '_cdc_ts' — expected DateTime64, got %s", cdcTsType))
	}

	// Check for CH columns that don't exist in ScyllaDB (extra columns are just informational)
	for _, c := range chCols {
		if c.Name == "_cdc_ts" {
			continue // already validated above
		}
		if _, exists := scyllaCols[c.Name]; !exists {
			warnings = append(warnings, fmt.Sprintf("column '%s' (%s) exists in ClickHouse but not in ScyllaDB — will always be NULL", c.Name, c.Type))
		}
	}

	return warnings, errors
}

// typeCompatible checks if the expected ClickHouse type (from CQL mapping) is compatible
// with the actual ClickHouse type. Handles Nullable wrapping differences.
func typeCompatible(expected, actual string) bool {
	// Normalize: strip Nullable() wrapper for comparison
	norm := func(t string) string {
		t = strings.TrimSpace(t)
		if strings.HasPrefix(t, "Nullable(") && strings.HasSuffix(t, ")") {
			t = t[len("Nullable(") : len(t)-1]
		}
		return strings.TrimSpace(t)
	}
	return norm(expected) == norm(actual)
}

// schemaCheckDetail returns a detailed column-by-column comparison for the API.
func schemaCheckDetail(scyllaSession *gocql.Session, keyspace, table, chFQN string, checker CHTableChecker, logger *log.Logger) map[string]interface{} {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	type colInfo struct {
		Name       string `json:"name"`
		ScyllaType string `json:"scylla_type,omitempty"`
		ExpectedCH string `json:"expected_ch,omitempty"`
		ActualCH   string `json:"actual_ch,omitempty"`
		Status     string `json:"status"` // "ok", "mismatch", "missing_in_ch", "missing_in_scylla", "cdc_meta"
	}

	// Get ScyllaDB columns
	scyllaCols := make(map[string]string)
	var scyllaOrder []string
	iter := scyllaSession.Query(
		"SELECT column_name, type FROM system_schema.columns WHERE keyspace_name = ? AND table_name = ?",
		keyspace, table,
	).WithContext(ctx).Iter()
	var colName, colType string
	for iter.Scan(&colName, &colType) {
		scyllaCols[colName] = colType
		scyllaOrder = append(scyllaOrder, colName)
	}
	if err := iter.Close(); err != nil {
		return map[string]interface{}{"ok": false, "error": fmt.Sprintf("ScyllaDB query error: %v", err)}
	}

	// Get ClickHouse columns
	chCols, err := checker.TableColumns(ctx, chFQN)
	if err != nil {
		return map[string]interface{}{"ok": false, "error": fmt.Sprintf("ClickHouse query error: %v", err)}
	}
	chColMap := make(map[string]string, len(chCols))
	for _, c := range chCols {
		chColMap[c.Name] = c.Type
	}

	var columns []colInfo
	cdcMetaCols := map[string]bool{"_cdc_ts": true}
	seen := make(map[string]bool)

	// ScyllaDB columns
	for _, name := range scyllaOrder {
		cqlType := scyllaCols[name]
		expectedCH := converter.CQLToClickHouseType(cqlType, false)
		seen[name] = true

		ci := colInfo{Name: name, ScyllaType: cqlType, ExpectedCH: expectedCH}
		if chType, exists := chColMap[name]; exists {
			ci.ActualCH = chType
			if typeCompatible(expectedCH, chType) {
				ci.Status = "ok"
			} else {
				ci.Status = "mismatch"
			}
		} else {
			ci.Status = "missing_in_ch"
		}
		columns = append(columns, ci)
	}

	// ClickHouse-only columns
	for _, c := range chCols {
		if seen[c.Name] {
			continue
		}
		ci := colInfo{Name: c.Name, ActualCH: c.Type}
		if cdcMetaCols[c.Name] {
			ci.Status = "cdc_meta"
		} else {
			ci.Status = "missing_in_scylla"
		}
		columns = append(columns, ci)
	}

	// Count issues
	mismatches, missingCH, missingScy := 0, 0, 0
	for _, c := range columns {
		switch c.Status {
		case "mismatch":
			mismatches++
		case "missing_in_ch":
			missingCH++
		case "missing_in_scylla":
			missingScy++
		}
	}

	return map[string]interface{}{
		"ok":                true,
		"scylla":            keyspace + "." + table,
		"clickhouse":        chFQN,
		"columns":           columns,
		"total_columns":     len(columns),
		"mismatches":        mismatches,
		"missing_in_ch":     missingCH,
		"missing_in_scylla": missingScy,
	}
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

// clearTableCheckpoint removes per-stream checkpoint rows for a table from the progress table.
// This forces the CDC reader to start fresh using the configured change_age_limit.
// The progress table is tiny (~10 rows per table) — this is NOT a production data table.
// PK is (generation, application_name, table_name, stream_id) — we scan with ALLOW FILTERING
// because the table has at most a few hundred rows total across all tables.
func clearTableCheckpoint(session *gocql.Session, progressTable, tableName string, logger *log.Logger) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// 1. Clear per-stream progress rows for this table
	iter := session.Query(
		fmt.Sprintf("SELECT generation, application_name, table_name, stream_id FROM %s WHERE application_name = ? AND table_name = ? ALLOW FILTERING", progressTable),
		"scylla2ch", tableName,
	).WithContext(ctx).Iter()

	var (
		gen     time.Time
		appName string
		tbl     string
		sid     []byte
		cleared int
	)
	for iter.Scan(&gen, &appName, &tbl, &sid) {
		if err := session.Query(
			fmt.Sprintf("DELETE FROM %s WHERE generation = ? AND application_name = ? AND table_name = ? AND stream_id = ?", progressTable),
			gen, appName, tbl, sid,
		).WithContext(ctx).Exec(); err != nil {
			logger.Printf("dashboard: clear checkpoint table=%s gen=%v: %v", tableName, gen, err)
		} else {
			cleared++
		}
	}
	if err := iter.Close(); err != nil {
		logger.Printf("dashboard: checkpoint scan table=%s: %v", tableName, err)
	}

	// 2. Clear the global generation + start time row (special partition).
	//    PK: (generation=zero-time, application_name="scylla2ch", table_name="", stream_id=empty).
	//    This forces scylla-cdc-go to use ChangeAgeLimit on next reader start.
	//    Safe: existing running tables are unaffected (they already have their reader running
	//    and only re-read the generation on restart, at which point they re-save it).
	if err := session.Query(
		fmt.Sprintf("DELETE FROM %s WHERE generation = ? AND application_name = ? AND table_name = ? AND stream_id = ?", progressTable),
		time.Time{}, "scylla2ch", "", []byte{},
	).WithContext(ctx).Exec(); err != nil {
		logger.Printf("dashboard: clear generation row: %v", err)
	} else {
		cleared++
	}

	logger.Printf("dashboard: cleared %d checkpoint rows for table=%s (reader will start fresh)", cleared, tableName)
}

