// validate.go — Pre-deploy connectivity test command.
//
// Tests connections to both ScyllaDB Cloud and ClickHouse Cloud:
//   1. ScyllaDB: connects to all configured keyspaces, verifies CDC log tables exist
//   2. ClickHouse: connects, verifies target tables exist
//   3. Prints summary with connection details and table counts
//
// Run this before deploying to catch config errors early:
//   scylla2ch validate --config /etc/scylla2ch/config.yaml
//
// Checks performed:
//   - ScyllaDB TLS connection (CA cert, auth, keyspace — per keyspace)
//   - CDC log companion tables present (ALTER TABLE WITH cdc done?)
//   - CDC postimage enabled (if configured)
//   - ClickHouse native TCP connection (TLS, auth, database)
//   - Target ClickHouse tables exist
package cmd

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/gocql/gocql"
	"github.com/spf13/cobra"

	"scylla2ch/config"
	"scylla2ch/writer"
)

func init() {
	rootCmd.AddCommand(validateCmd)
}

var validateCmd = &cobra.Command{
	Use:   "validate",
	Short: "Test connectivity to ScyllaDB Cloud and ClickHouse Cloud",
	RunE:  runValidate,
}

func runValidate(cmd *cobra.Command, args []string) error {
	cfg, err := config.Load(cfgFile)
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}

	logger := log.New(os.Stderr, "[validate] ", log.Ldate|log.Ltime)

	// 1. Test ScyllaDB connection — validate all keyspaces
	keyspaces := cfg.Scylla.AllKeyspaces()
	logger.Printf("testing ScyllaDB Cloud connection for %d keyspaces: %v", len(keyspaces), keyspaces)

	var allSessions []*gocql.Session
	defer func() {
		for _, s := range allSessions {
			s.Close()
		}
	}()

	for _, ks := range keyspaces {
		session, err := connectScylla(cfg, ks, logger)
		if err != nil {
			return fmt.Errorf("ScyllaDB FAILED keyspace=%s: %w", ks, err)
		}
		allSessions = append(allSessions, session)

		// Verify CDC is enabled on configured tables
		for i := range cfg.Tables.Explicit {
			tableName := cfg.Tables.Explicit[i].Name
			cdcLogTable := fmt.Sprintf("%s_scylla_cdc_log", tableName)

			iter := session.Query(
				"SELECT table_name FROM system_schema.tables WHERE keyspace_name = ? AND table_name = ?",
				ks, cdcLogTable,
			).Iter()

			var found string
			if iter.Scan(&found) {
				logger.Printf("  OK: %s.%s CDC log table exists (%s_scylla_cdc_log)", ks, tableName, tableName)
			} else {
				logger.Printf("  WARN: %s.%s CDC log table NOT FOUND — run ALTER TABLE WITH cdc first", ks, tableName)
			}
			if err := iter.Close(); err != nil {
				logger.Printf("  ERROR closing iterator for %s: %v", tableName, err)
			}
		}

		// Check postimage is enabled
		iter := session.Query(
			"SELECT table_name, extensions FROM system_schema.tables WHERE keyspace_name = ?",
			ks,
		).Iter()

		var tableName string
		var extensions map[string][]byte
		for iter.Scan(&tableName, &extensions) {
			if cdcConfig, ok := extensions["cdc"]; ok {
				hasPostimage := false
				cdcStr := string(cdcConfig)
				if strings.Contains(cdcStr, "postimage") && strings.Contains(cdcStr, "true") {
					hasPostimage = true
				}
				if !hasPostimage {
					logger.Printf("  WARN: %s.%s has CDC enabled but postimage may not be ON — verify with DESC TABLE", ks, tableName)
				}
			}
		}
		if err := iter.Close(); err != nil {
			logger.Printf("  ERROR closing CDC check iterator: %v", err)
		}
	}

	logger.Printf("ScyllaDB Cloud: OK (%d keyspaces)", len(keyspaces))

	// 2. Test ClickHouse connection
	logger.Printf("testing ClickHouse Cloud connection...")
	chWriter, err := writer.NewClickHouseWriter(cfg.ClickHouse, cfg.Batch, logger, Version)
	if err != nil {
		return fmt.Errorf("ClickHouse FAILED: %w", err)
	}
	defer chWriter.Close()

	// Verify target tables exist
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for i := range cfg.Tables.Explicit {
		targets := []string{cfg.Tables.Explicit[i].Name}
		for _, r := range cfg.Tables.Explicit[i].Routes {
			targets = append(targets, r.Target)
		}
		for _, target := range targets {
			exists, err := chWriter.TableExists(ctx, target)
			if err != nil {
				logger.Printf("  ERROR checking %s: %v", target, err)
			} else if exists {
				logger.Printf("  OK: %s.%s exists in ClickHouse", cfg.ClickHouse.Database, target)
			} else {
				logger.Printf("  WARN: %s.%s does NOT exist in ClickHouse — create it first", cfg.ClickHouse.Database, target)
			}
		}
	}

	logger.Printf("ClickHouse Cloud: OK")

	// 3. Summary
	fmt.Println()
	fmt.Println("=== Validation Summary ===")
	fmt.Printf("ScyllaDB Cloud:  Connected (%s) — %d keyspaces: %v\n", cfg.Scylla.Hosts[0], len(keyspaces), keyspaces)
	fmt.Printf("ClickHouse Cloud: Connected (%s:%d)\n", cfg.ClickHouse.Host, cfg.ClickHouse.Port)
	if cfg.Tables.AutoDiscover {
		fmt.Printf("Tables: auto-discover (hot-reload every %s)\n", cfg.Tables.DiscoverInterval)
	} else {
		fmt.Printf("Tables configured: %d\n", len(cfg.Tables.Explicit))
	}
	fmt.Printf("CDC mode: %s\n", cfg.CDC.Mode)
	fmt.Println()
	fmt.Println("NOTE: scylla2ch reads ONLY from *_scylla_cdc_log tables.")
	fmt.Println("      These are separate from your production tables — ZERO load on original data.")
	fmt.Println()
	fmt.Println("Ready to run: scylla2ch run --config config.yaml")

	return nil
}
