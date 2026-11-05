// run.go — Main CDC streaming command.
//
// This is the core of scylla2ch. It:
//   1. Connects to ScyllaDB Cloud (TLS, CA cert, TokenAware routing)
//   2. Connects to ClickHouse Cloud (native TCP, LZ4 compression)
//   3. Auto-discovers CDC-enabled tables (scans system_schema for *_scylla_cdc_log)
//   4. Auto-creates ClickHouse tables from ScyllaDB schema (ReplacingMergeTree)
//   5. Starts one CDC reader goroutine per table (scylla-cdc-go library)
//   6. Hot-reloads new CDC tables without restart (periodic scan)
//   7. SIGHUP config reload: re-reads config, starts readers for new tables (zero downtime)
//   8. Handles graceful shutdown (SIGINT/SIGTERM → drain all buffers → exit)
//
// Multi-keyspace support:
//   - One gocql.Session per keyspace (gocql sessions are keyspace-bound)
//   - Each keyspace maps to a ClickHouse database (same name by default)
//   - Each keyspace has its own progress table (<keyspace>.scylla2ch_progress)
//   - Auto-discover/auto-create/hot-reload operate on ALL keyspaces
//
// Data flow:
//   ScyllaDB *_scylla_cdc_log  →  scylla-cdc-go Reader  →  Consumer.Consume()
//     →  ConvertValue() (type mapping)  →  ClickHouseWriter.Add() (channel send)
//     →  tableWriter goroutine (batch + timer)  →  ClickHouse INSERT
//
// Key design: ZERO reads hit production tables. Only CDC log tables are read.
// CDC log tables are separate system tables maintained by ScyllaDB's CDC subsystem.
//
// Also contains connectScylla() — shared by run, snapshot, and validate commands.
package cmd

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/gocql/gocql"
	scyllacdc "github.com/scylladb/scylla-cdc-go"
	"github.com/spf13/cobra"

	"scylla2ch/config"
	"scylla2ch/consumer"
	"scylla2ch/converter"
	"scylla2ch/dashboard"
	"scylla2ch/metrics"
	"scylla2ch/writer"
)

func init() {
	rootCmd.AddCommand(runCmd)
}

var runCmd = &cobra.Command{
	Use:   "run",
	Short: "Start CDC streaming from ScyllaDB to ClickHouse",
	RunE:  runCDC,
}

// ksState holds per-keyspace state: the gocql session and ClickHouse database mapping.
type ksState struct {
	keyspace string
	session  *gocql.Session
	chDB     string // ClickHouse database name for this keyspace
}

func runCDC(cmd *cobra.Command, args []string) error {
	cfg, err := config.Load(cfgFile)
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}

	logger := log.New(os.Stderr, "[scylla2ch] ", log.Ldate|log.Ltime|log.Lmicroseconds)

	metrics.SetBuildInfo(Version)

	// Security warnings — log when TLS is off for non-localhost connections
	isLocalhost := func(host string) bool {
		h, _, err := net.SplitHostPort(host)
		if err != nil {
			h = host
		}
		return h == "localhost" || h == "127.0.0.1" || h == "::1"
	}

	if !cfg.Scylla.TLS {
		allLocal := true
		for _, h := range cfg.Scylla.Hosts {
			if !isLocalhost(h) {
				allLocal = false
				break
			}
		}
		if !allLocal {
			logger.Printf("SECURITY WARNING: scylla.tls is disabled — credentials sent in PLAINTEXT over network")
		}
	}

	if !cfg.ClickHouse.TLS && !isLocalhost(cfg.ClickHouse.Host) {
		logger.Printf("SECURITY WARNING: clickhouse.tls is disabled — credentials sent in PLAINTEXT over network")
	}

	if cfg.Scylla.TLSSkipVerify {
		logger.Printf("SECURITY WARNING: scylla.tls_skip_verify is enabled — TLS encrypted but certificates NOT verified (dev only)")
	}

	keyspaces := cfg.Scylla.AllKeyspaces()
	logger.Printf("keyspaces: %v", keyspaces)

	// 0. Start dashboard early — serves UI even when databases are unreachable.
	//    This lets operators see config, add/remove tables, and diagnose connectivity.
	var (
		running   = make(map[string]bool)
		runningMu sync.Mutex
	)
	startTime := time.Now()
	dashState := &dashboard.State{
		Running:    &running,
		RunningMu:  &runningMu,
		Config:     cfg,
		ConfigFile: cfgFile,
		StartTime:  startTime,
		Version:    Version,
		Logger:     logger,
		SessionMap: nil,
		CHDBMap:    nil,
		CHChecker:  nil,
	}
	earlyHealthServer := &http.Server{
		Addr:              fmt.Sprintf(":%d", cfg.Health.Port),
		Handler:           dashboard.NewMux(dashState),
		ReadHeaderTimeout: 5 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	go func() {
		logger.Printf("dashboard + health endpoint listening on :%d (pre-connect)", cfg.Health.Port)
		if err := earlyHealthServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Printf("health server error: %v", err)
		}
	}()

	// 1. Connect to ScyllaDB — one session per keyspace (gocql sessions are keyspace-bound)
	var sessions []ksState
	for _, ks := range keyspaces {
		sess, err := connectScylla(cfg, ks, logger)
		if err != nil {
			for _, s := range sessions {
				s.session.Close()
			}
			return fmt.Errorf("scylla connect keyspace=%s: %w", ks, err)
		}

		// ClickHouse database = keyspace name by default.
		// For single-keyspace with clickhouse.database set, use that (backward compat).
		chDB := ks
		if len(keyspaces) == 1 && cfg.ClickHouse.Database != "" {
			chDB = cfg.ClickHouse.Database
		}

		sessions = append(sessions, ksState{keyspace: ks, session: sess, chDB: chDB})
	}
	defer func() {
		for _, s := range sessions {
			s.session.Close()
		}
		metrics.ScyllaConnected.Set(0)
	}()

	// 2. Connect to ClickHouse
	chWriter, err := writer.NewClickHouseWriter(cfg.ClickHouse, cfg.Batch, logger, Version)
	if err != nil {
		return fmt.Errorf("clickhouse connect: %w", err)
	}
	defer func() {
		chWriter.Close()
		metrics.ClickHouseConnected.Set(0)
	}()
	logger.Printf("connected to ClickHouse host=%s", cfg.ClickHouse.Host)
	metrics.ScyllaConnected.Set(1)
	metrics.ClickHouseConnected.Set(1)

	// Ensure ClickHouse databases exist for all keyspaces
	for _, s := range sessions {
		ensureCtx, ensureCancel := context.WithTimeout(context.Background(), 10*time.Second)
		if err := chWriter.EnsureDatabase(ensureCtx, s.chDB); err != nil {
			logger.Printf("WARN auto-create database %s: %v", s.chDB, err)
		}
		ensureCancel()
	}

	// 3. Build initial table list — scan all keyspaces
	var tableNames []string
	disc := converter.NewDiscoverer(logger)

	// Build keyspace→session lookup maps
	sessionMap := make(map[string]*gocql.Session, len(sessions))
	chDBMap := make(map[string]string, len(sessions))
	for _, s := range sessions {
		sessionMap[s.keyspace] = s.session
		chDBMap[s.keyspace] = s.chDB
	}

	if cfg.Tables.AutoDiscover {
		for _, s := range sessions {
			discovered, err := disc.ScanForCDCTables(s.session, s.keyspace)
			if err != nil {
				return fmt.Errorf("auto-discover keyspace=%s: %w", s.keyspace, err)
			}
			logger.Printf("auto-discovered %d CDC tables in %s: %v", len(discovered), s.keyspace, discovered)

			if cfg.Tables.AutoCreate {
				chConn := chWriter.Conn()
				for _, t := range discovered {
					if err := disc.AutoCreateCHTable(chConn, s.session, s.keyspace, s.chDB, t); err != nil {
						logger.Printf("WARN auto-create table=%s.%s err=%v (may already exist)", s.keyspace, t, err)
					}
				}
			}

			// When auto_create is off, verify each table exists in ClickHouse
			checkCtx, checkCancel := context.WithTimeout(context.Background(), 30*time.Second)
			for _, t := range discovered {
				if !cfg.Tables.AutoCreate {
					exists, err := chWriter.TableExists(checkCtx, fmt.Sprintf("%s.%s", s.chDB, t))
					if err != nil {
						logger.Printf("WARN cannot check ClickHouse table=%s.%s: %v — will try anyway", s.chDB, t, err)
					} else if !exists {
						logger.Printf("ERROR ClickHouse table=%s.%s does not exist — SKIPPING (create it manually, auto_create is off)", s.chDB, t)
						metrics.ErrorsTotal.WithLabelValues("discovery", "error").Inc()
						continue
					}
				}
				tableNames = append(tableNames, fmt.Sprintf("%s.%s", s.keyspace, t))
			}
			checkCancel()
		}
	} else {
		// Explicit tables: always start readers. If ClickHouse table doesn't exist yet,
		// the writer pauses (like Kafka consumer.pause) — rows held in bounded buffer,
		// checkpoint frozen, no data loss. When table is created, writer resumes automatically.
		tableNames = cfg.TableNames()
	}

	if len(tableNames) == 0 {
		return fmt.Errorf("no tables configured — add tables in config or enable auto_discover")
	}

	// 4. Start metrics server
	metricsServer := metrics.NewServer(cfg.Metrics)
	go func() {
		logger.Printf("metrics server listening on :%d%s", cfg.Metrics.Port, cfg.Metrics.Path)
		if err := metricsServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Printf("metrics server error: %v", err)
		}
	}()

	// 5. Upgrade dashboard with live DB references (early server already running)
	dashState.SessionMap = sessionMap
	dashState.CHDBMap = chDBMap
	dashState.CHChecker = chWriter
	healthServer := earlyHealthServer

	defer func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer shutdownCancel()
		if err := metricsServer.Shutdown(shutdownCtx); err != nil {
			logger.Printf("metrics server shutdown: %v", err)
		}
		if err := healthServer.Shutdown(shutdownCtx); err != nil {
			logger.Printf("health server shutdown: %v", err)
		}
	}()

	// 6. Graceful shutdown context
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// Track uptime (startTime set above when creating dashboard state)
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				metrics.UptimeSeconds.Set(time.Since(startTime).Seconds())
			}
		}
	}()

	// 7. Reader management — supports hot-reload of new tables without restart.
	//
	// How it works (like PeerDB):
	//   1. Enable CDC on a ScyllaDB table: ALTER TABLE x WITH cdc = {'enabled': true}
	//   2. scylla2ch auto-detects it within discover_interval (default 30s)
	//   3. Auto-creates the ClickHouse table from ScyllaDB schema
	//   4. Starts a CDC reader — data flows immediately, no restart needed
	factory := consumer.NewFactory(cfg, chWriter, logger)
	for _, s := range sessions {
		factory.SetKeyspaceDB(s.keyspace, s.chDB)
	}

	var (
		readerWg sync.WaitGroup // tracks all reader goroutines
		firstErr error
		errOnce  sync.Once
	)

	// startReader launches a CDC reader goroutine for one table.
	// Each table uses the session for its keyspace and its own progress table.
	startReader := func(sess *gocql.Session, keyspace, tableName string) error {
		progressTable := fmt.Sprintf("%s.%s", keyspace, config.ProgressTable)
		progressMgr, err := scyllacdc.NewTableBackedProgressManager(sess, progressTable, "scylla2ch")
		if err != nil {
			return fmt.Errorf("progress manager for %s: %w", tableName, err)
		}

		readerCfg := &scyllacdc.ReaderConfig{
			Session:               sess,
			TableNames:            []string{tableName},
			ChangeConsumerFactory: factory,
			ProgressManager:       progressMgr,
			Logger:                logger,
			Advanced: scyllacdc.AdvancedReaderConfig{
				ConfidenceWindowSize:   cfg.CDC.ConfidenceWindow,
				QueryTimeWindowSize:    cfg.CDC.QueryWindow,
				ChangeAgeLimit:         cfg.CDC.ChangeAgeLimit,
				PostEmptyQueryDelay:    1 * time.Second,
				PostNonEmptyQueryDelay: 1 * time.Second,
			},
		}

		reader, err := scyllacdc.NewReader(ctx, readerCfg)
		if err != nil {
			return fmt.Errorf("cdc reader %s: %w", tableName, err)
		}

		logger.Printf("starting CDC reader for %s", tableName)
		readerWg.Add(1)
		go func() {
			defer readerWg.Done()
			defer metrics.TablesActive.Dec()
			if err := reader.Run(ctx); err != nil {
				logger.Printf("ERROR reader %s: %v", tableName, err)
				metrics.ReaderErrorsTotal.WithLabelValues(tableName).Inc()
				metrics.ErrorsTotal.WithLabelValues("reader", "error").Inc()
				errOnce.Do(func() {
					firstErr = fmt.Errorf("reader %s: %w", tableName, err)
					cancel() // signal all readers + discovery to stop
				})
			}
		}()

		metrics.TablesActive.Inc()
		return nil
	}

	// Start initial readers (hold mutex to prevent race with hot-reload goroutine)
	runningMu.Lock()
	for _, tableName := range tableNames {
		// Extract keyspace from "keyspace.table" to find the right session
		dotIdx := strings.IndexByte(tableName, '.')
		if dotIdx == -1 {
			runningMu.Unlock()
			return fmt.Errorf("table name %q missing keyspace prefix (expected keyspace.table)", tableName)
		}
		ks := tableName[:dotIdx]
		sess := sessionMap[ks]
		if sess == nil {
			runningMu.Unlock()
			return fmt.Errorf("no session for keyspace %s (table %s)", ks, tableName)
		}
		if err := startReader(sess, ks, tableName); err != nil {
			runningMu.Unlock()
			return err
		}
		running[tableName] = true
	}
	runningMu.Unlock()
	logger.Printf("started %d CDC readers across %d keyspaces", len(tableNames), len(keyspaces))

	// 8. SIGHUP config reload: re-read config file and start readers for NEW tables only.
	// Read-only reload — existing readers/sessions are NOT modified (safe, no data races).
	// New keyspaces require a full restart (new gocql sessions needed).
	sighupCh := make(chan os.Signal, 1)
	signal.Notify(sighupCh, syscall.SIGHUP)
	readerWg.Add(1)
	go func() {
		defer readerWg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case <-sighupCh:
				reloadConfig(cfgFile, cfg, disc, chWriter, sessionMap, chDBMap,
					&running, &runningMu, startReader, ctx, logger)
			}
		}
	}()

	// 9. Hot-reload: background goroutine periodically scans ALL keyspaces for new CDC tables.
	// Just like PeerDB — enable CDC on ScyllaDB, table appears in ClickHouse automatically.
	if cfg.Tables.AutoDiscover && cfg.Tables.DiscoverInterval > 0 {
		readerWg.Add(1)
		go func() {
			defer readerWg.Done()
			ticker := time.NewTicker(cfg.Tables.DiscoverInterval)
			defer ticker.Stop()
			logger.Printf("hot-reload enabled: scanning %d keyspaces for new CDC tables every %s", len(sessions), cfg.Tables.DiscoverInterval)

			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					for _, s := range sessions {
						discovered, err := disc.ScanForCDCTables(s.session, s.keyspace)
						if err != nil {
							logger.Printf("WARN hot-reload scan keyspace=%s: %v", s.keyspace, err)
							metrics.ErrorsTotal.WithLabelValues("discovery", "warning").Inc()
							continue
						}

						for _, t := range discovered {
							fqn := fmt.Sprintf("%s.%s", s.keyspace, t)

							runningMu.Lock()
							alreadyRunning := running[fqn]
							if !alreadyRunning {
								running[fqn] = true // claim immediately to prevent double-start
							}
							runningMu.Unlock()

							if alreadyRunning {
								continue
							}

							// New table detected — auto-create ClickHouse table + start reader
							logger.Printf("HOT-RELOAD: new CDC table detected: %s", fqn)

							if cfg.Tables.AutoCreate {
								if err := disc.AutoCreateCHTable(chWriter.Conn(), s.session, s.keyspace, s.chDB, t); err != nil {
									logger.Printf("WARN hot-reload auto-create %s: %v (may already exist)", t, err)
								}
							} else {
								// Verify ClickHouse table exists before starting reader
								checkCtx, checkCancel := context.WithTimeout(ctx, 10*time.Second)
								exists, chErr := chWriter.TableExists(checkCtx, fmt.Sprintf("%s.%s", s.chDB, t))
								checkCancel()
								if chErr != nil {
									logger.Printf("WARN hot-reload cannot check ClickHouse table=%s.%s: %v", s.chDB, t, chErr)
								} else if !exists {
									logger.Printf("ERROR hot-reload: ClickHouse table %s.%s does not exist — SKIPPING", s.chDB, t)
									metrics.ErrorsTotal.WithLabelValues("discovery", "error").Inc()
									runningMu.Lock()
									delete(running, fqn)
									runningMu.Unlock()
									continue
								}
							}

							if err := startReader(s.session, s.keyspace, fqn); err != nil {
								logger.Printf("ERROR hot-reload start reader %s: %v", fqn, err)
								metrics.ErrorsTotal.WithLabelValues("reader", "error").Inc()
								runningMu.Lock()
								delete(running, fqn)
								runningMu.Unlock()
							} else {
								metrics.HotReloadTotal.Inc()
								logger.Printf("HOT-RELOAD: started CDC reader for %s — data flowing to ClickHouse", fqn)
							}
						}
					}
				}
			}
		}()
	}

	// Wait for all readers to finish (they run until ctx is cancelled by signal or error).
	readerWg.Wait()

	if firstErr != nil {
		logger.Printf("shutting down due to reader error: %v", firstErr)
	} else {
		logger.Printf("shutdown complete — all readers stopped cleanly")
	}
	return firstErr
}

// connectScylla creates a gocql.Session for one keyspace.
// Shared by run, snapshot, and validate commands.
func connectScylla(cfg *config.Config, keyspace string, logger *log.Logger) (*gocql.Session, error) {
	// Strip port from hostnames (gocql uses cluster.Port, not host:port).
	// Uses net.SplitHostPort for correct IPv6 handling (e.g., [::1]:9142).
	hosts := make([]string, len(cfg.Scylla.Hosts))
	for i, h := range cfg.Scylla.Hosts {
		host, _, err := net.SplitHostPort(h)
		if err != nil {
			hosts[i] = h // no port suffix, use as-is
		} else {
			hosts[i] = host
		}
	}
	cluster := gocql.NewCluster(hosts...)
	if cfg.Scylla.Port > 0 {
		cluster.Port = cfg.Scylla.Port
	} else {
		cluster.Port = 9142 // ScyllaDB Cloud default TLS port
	}
	cluster.Keyspace = keyspace
	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: cfg.Scylla.Username,
		Password: cfg.Scylla.Password,
	}
	if cfg.Scylla.TLS {
		tlsConfig := &tls.Config{
			InsecureSkipVerify: cfg.Scylla.TLSSkipVerify, // dev: skip CA chain verify (still encrypted)
		}

		// Load CA cert — supports 3 methods:
		// 1. ca_cert_base64: base64-encoded PEM (for Kubernetes secrets / env vars)
		// 2. ca_cert_path: path to PEM file
		// 3. Neither: uses system CA pool (works if ScyllaDB uses a public CA)
		caCertPool := x509.NewCertPool()
		certLoaded := false

		if cfg.Scylla.CACertBase64 != "" {
			caCert, err := base64.StdEncoding.DecodeString(cfg.Scylla.CACertBase64)
			if err != nil {
				return nil, fmt.Errorf("decode ca_cert_base64: %w", err)
			}
			if !caCertPool.AppendCertsFromPEM(caCert) {
				return nil, fmt.Errorf("failed to parse CA cert from ca_cert_base64")
			}
			certLoaded = true
			logger.Printf("loaded ScyllaDB CA cert from base64 env")
		} else if cfg.Scylla.CACertPath != "" {
			caCert, err := os.ReadFile(cfg.Scylla.CACertPath)
			if err != nil {
				return nil, fmt.Errorf("read CA cert %s: %w", cfg.Scylla.CACertPath, err)
			}
			if !caCertPool.AppendCertsFromPEM(caCert) {
				return nil, fmt.Errorf("failed to parse CA cert from %s", cfg.Scylla.CACertPath)
			}
			certLoaded = true
			logger.Printf("loaded ScyllaDB CA cert from %s", cfg.Scylla.CACertPath)
		}

		if certLoaded {
			tlsConfig.RootCAs = caCertPool
		}

		cluster.SslOpts = &gocql.SslOptions{
			Config:                 tlsConfig,
			EnableHostVerification: false, // ScyllaDB Cloud DNS doesn't match node cert CN
		}
	}
	// --- Consistency ---
	// LOCAL_QUORUM: safe for checkpoint writes (durable across replicas).
	// CDC log reads are immutable, so LOCAL_ONE would work too, but we keep
	// QUORUM because the same session is used for checkpoint table writes
	// by scylla-cdc-go's TableBackedProgressManager.
	switch strings.ToUpper(cfg.Scylla.Consistency) {
	case "LOCAL_ONE", "LOCALONE":
		cluster.Consistency = gocql.LocalOne
	case "ONE":
		cluster.Consistency = gocql.One
	default:
		cluster.Consistency = gocql.LocalQuorum
	}

	// --- Connection pool ---
	// 1 connection per host is enough for CDC reads (sequential per-stream,
	// not user-facing). Halves the connection count on ScyllaDB vs default 2.
	cluster.NumConns = 1

	// --- Peer discovery ---
	// When connecting via public IPs (ScyllaDB Cloud without VPC peering),
	// system.peers returns private IPs that are unreachable. gocql v1.17.3
	// panics (nil Conn in UseKeyspace/initConnPicker) when connecting to them.
	// Fix: allow ONLY the contact point IPs we provided, reject discovered peers.
	if cfg.Scylla.TLSSkipVerify {
		cluster.IgnorePeerAddr = true
		cluster.DisableInitialHostLookup = true
		cluster.Events.DisableNodeStatusEvents = true
		cluster.Events.DisableTopologyEvents = true
		cluster.Events.DisableSchemaEvents = true
		// Build allowlist of contact point IPs (resolved from DNS)
		allowedIPs := make(map[string]bool, len(hosts))
		for _, h := range hosts {
			ips, err := net.LookupHost(h)
			if err == nil {
				for _, ip := range ips {
					allowedIPs[ip] = true
				}
			}
			allowedIPs[h] = true // also allow the hostname itself
		}
		cluster.HostFilter = gocql.HostFilterFunc(func(host *gocql.HostInfo) bool {
			return allowedIPs[host.ConnectAddress().String()] ||
				allowedIPs[host.Peer().String()] ||
				allowedIPs[host.BroadcastAddress().String()]
		})
		logger.Printf("host filter: allowing only contact point IPs %v", allowedIPs)
	}

	// --- Timeouts ---
	// ConnectTimeout: how long to wait for TCP + TLS handshake per host.
	// Timeout: per-query timeout (CDC log reads + checkpoint writes).
	// ScyllaDB Cloud has higher latency than local clusters — 10s is safe.
	cluster.ConnectTimeout = 10 * time.Second
	cluster.Timeout = 10 * time.Second

	// --- Keepalive ---
	// TCP keepalive prevents cloud load balancers (AWS NLB: 350s idle timeout)
	// from silently dropping idle connections. 15s is well under any LB timeout.
	cluster.SocketKeepalive = 15 * time.Second

	// --- Compression ---
	// Snappy compression reduces CDC log read bandwidth by ~60%.
	// CDC rows are text-heavy (UUIDs, strings, timestamps) — compresses well.
	// Same as Python backend: compression=True (which defaults to Snappy/LZ4).
	cluster.Compressor = gocql.SnappyCompressor{}

	// --- Reconnection ---
	// Exponential backoff: 1s → 2s → 4s → ... → 60s max.
	// Avoids hammering ScyllaDB during a node restart or network partition.
	// Same pattern as Python backend: ExponentialReconnectionPolicy(1s, 60s).
	cluster.ReconnectionPolicy = &gocql.ExponentialReconnectionPolicy{
		MaxRetries:      10, // retries per initial connect attempt (gocql uses this as loop bound)
		InitialInterval: 1 * time.Second,
		MaxInterval:     60 * time.Second,
	}

	// --- Load balancing ---
	// TokenAware routes CDC log queries to the node holding the token range.
	// DCAware ensures we only talk to nodes in our datacenter.
	// This is the same policy as the Python backend: TokenAwarePolicy(DCAwareRoundRobin).
	//
	// When DisableInitialHostLookup is true (public IP access), gocql doesn't
	// query system.local/peers so the DC topology is empty. Use plain RoundRobin
	// to avoid "datacenter not found in topology" errors. All ScyllaDB Cloud
	// nodes in a cluster are same DC anyway.
	if cfg.Scylla.DC != "" && !cfg.Scylla.TLSSkipVerify {
		cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(
			gocql.DCAwareRoundRobinPolicy(cfg.Scylla.DC),
		)
	} else {
		cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(
			gocql.RoundRobinHostPolicy(),
		)
	}

	// --- Page size ---
	// 5000 rows per page (default). Good for CDC: large enough to reduce
	// round-trips, small enough to not cause GC pressure on ScyllaDB nodes.
	cluster.PageSize = 5000

	session, err := cluster.CreateSession()
	if err != nil {
		return nil, fmt.Errorf("scylla connect keyspace=%s: %w", keyspace, err)
	}
	logger.Printf("connected to ScyllaDB hosts=%v keyspace=%s dc=%s conns_per_host=1 compression=snappy consistency=%s",
		cfg.Scylla.Hosts, keyspace, cfg.Scylla.DC, cluster.Consistency)
	return session, nil
}

// reloadConfig handles SIGHUP: loads new config, starts readers for NEW tables only.
// Read-only reload — existing readers/sessions are NOT modified.
// Only tables in existing keyspaces can be added; new keyspaces require a restart.
func reloadConfig(
	cfgFile string,
	oldCfg *config.Config,
	disc *converter.Discoverer,
	chWriter *writer.ClickHouseWriter,
	sessionMap map[string]*gocql.Session,
	chDBMap map[string]string,
	running *map[string]bool,
	runningMu *sync.Mutex,
	startReader func(sess *gocql.Session, keyspace, tableName string) error,
	ctx context.Context,
	logger *log.Logger,
) {
	logger.Printf("SIGHUP received — reloading config from %s", cfgFile)
	metrics.SighupReloadTotal.Inc()

	newCfg, err := config.Load(cfgFile)
	if err != nil {
		logger.Printf("WARN SIGHUP reload: config parse error: %v — keeping current config", err)
		metrics.ErrorsTotal.WithLabelValues("reload", "warning").Inc()
		return
	}

	// Build new table list
	var newTables []string
	if newCfg.Tables.AutoDiscover {
		// Auto-discover: scan each known keyspace for CDC tables
		for ks, sess := range sessionMap {
			discovered, err := disc.ScanForCDCTables(sess, ks)
			if err != nil {
				logger.Printf("WARN SIGHUP reload: scan keyspace=%s: %v", ks, err)
				metrics.ErrorsTotal.WithLabelValues("reload", "warning").Inc()
				continue
			}
			for _, t := range discovered {
				newTables = append(newTables, fmt.Sprintf("%s.%s", ks, t))
			}
		}
	} else {
		newTables = newCfg.TableNames()
	}

	// Warn about new keyspaces that can't be added without restart
	for _, ks := range newCfg.Scylla.AllKeyspaces() {
		if sessionMap[ks] == nil {
			logger.Printf("WARN SIGHUP reload: keyspace %q not in session map — requires restart to add new keyspaces", ks)
		}
	}

	// Start readers for new tables only
	started := 0
	for _, fqn := range newTables {
		runningMu.Lock()
		alreadyRunning := (*running)[fqn]
		if !alreadyRunning {
			(*running)[fqn] = true // claim to prevent double-start
		}
		runningMu.Unlock()

		if alreadyRunning {
			continue
		}

		// Extract keyspace
		dotIdx := strings.IndexByte(fqn, '.')
		if dotIdx == -1 {
			logger.Printf("WARN SIGHUP reload: table %q missing keyspace prefix — skipping", fqn)
			continue
		}
		ks := fqn[:dotIdx]
		tbl := fqn[dotIdx+1:]

		sess := sessionMap[ks]
		if sess == nil {
			logger.Printf("WARN SIGHUP reload: no session for keyspace %s (table %s) — requires restart", ks, fqn)
			runningMu.Lock()
			delete(*running, fqn)
			runningMu.Unlock()
			continue
		}

		chDB := chDBMap[ks]

		// Auto-create ClickHouse table if enabled
		if newCfg.Tables.AutoCreate {
			if err := disc.AutoCreateCHTable(chWriter.Conn(), sess, ks, chDB, tbl); err != nil {
				logger.Printf("WARN SIGHUP reload: auto-create %s: %v (may already exist)", fqn, err)
			}
		} else {
			// Verify ClickHouse table exists
			checkCtx, checkCancel := context.WithTimeout(ctx, 10*time.Second)
			exists, chErr := chWriter.TableExists(checkCtx, fmt.Sprintf("%s.%s", chDB, tbl))
			checkCancel()
			if chErr != nil {
				logger.Printf("WARN SIGHUP reload: cannot check ClickHouse table=%s.%s: %v", chDB, tbl, chErr)
			} else if !exists {
				logger.Printf("ERROR SIGHUP reload: ClickHouse table %s.%s does not exist — SKIPPING", chDB, tbl)
				metrics.ErrorsTotal.WithLabelValues("reload", "error").Inc()
				runningMu.Lock()
				delete(*running, fqn)
				runningMu.Unlock()
				continue
			}
		}

		if err := startReader(sess, ks, fqn); err != nil {
			logger.Printf("ERROR SIGHUP reload: start reader %s: %v", fqn, err)
			metrics.ErrorsTotal.WithLabelValues("reader", "error").Inc()
			runningMu.Lock()
			delete(*running, fqn)
			runningMu.Unlock()
		} else {
			started++
			metrics.HotReloadTotal.Inc()
			logger.Printf("SIGHUP reload: started CDC reader for %s", fqn)
		}
	}

	runningMu.Lock()
	total := len(*running)
	runningMu.Unlock()
	logger.Printf("SIGHUP reload complete: started %d new readers, %d tables total", started, total)
}
