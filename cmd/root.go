// Package cmd implements the CLI subcommands for scylla2ch.
//
// Subcommands:
//   - run:      Main CDC streaming loop (cmd/run.go)
//   - snapshot: Bulk-load ScyllaDB table → ClickHouse (cmd/snapshot.go)
//   - validate: Connectivity test (cmd/validate.go)
//
// All commands share connectScylla() (in run.go) for ScyllaDB session creation,
// including TLS, CA cert loading (file or base64), and TokenAware routing.
package cmd

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime/debug"

	"github.com/joho/godotenv"
	"github.com/spf13/cobra"
)

var cfgFile string

// Version is set at build time via ldflags: -X scylla2ch/cmd.Version=<version>
// Falls back to git commit from build info when not set.
var Version = "dev"

func init() {
	if Version == "dev" {
		if info, ok := debug.ReadBuildInfo(); ok {
			var rev, dirty string
			for _, s := range info.Settings {
				switch s.Key {
				case "vcs.revision":
					if len(s.Value) >= 7 {
						rev = s.Value[:7]
					}
				case "vcs.modified":
					if s.Value == "true" {
						dirty = "-dirty"
					}
				}
			}
			if rev != "" {
				Version = "1.0.0-" + rev + dirty
			}
		}
	}
}

var rootCmd = &cobra.Command{
	Use:   "scylla2ch",
	Short: "Direct ScyllaDB CDC → ClickHouse replicator",
	Long: `scylla2ch reads ScyllaDB CDC log tables directly and writes to ClickHouse
via native TCP. Zero Kafka, zero Debezium, zero Python consumers.

Features:
  - Auto-discover CDC-enabled tables (zero config)
  - Auto-create ClickHouse tables from ScyllaDB schema
  - Dynamic type mapping (all CQL types → ClickHouse)
  - Channel-per-table architecture (lock-free, back-pressure)
  - Exponential retry with poison-row protection
  - Prometheus metrics (throughput, latency, lag, drops)
  - Graceful shutdown with full buffer drain
  - Multi-target routing (1 ScyllaDB table → N ClickHouse tables)
  - CA cert from file or base64 env var (Kubernetes-ready)`,
}

func init() {
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "/etc/scylla2ch/config.yaml", "config file path")

	// Auto-load .env.prod from the config file's directory, then from working directory.
	// Environment variables from .env.prod are set BEFORE config.Load() expands ${VAR}.
	// Existing env vars (e.g. from systemd EnvironmentFile) take precedence — godotenv
	// does NOT overwrite already-set variables.
	loadEnvFile()
}

func loadEnvFile() {
	// Try config directory first (e.g. /etc/scylla2ch/.env.prod)
	cfgDir := filepath.Dir(cfgFile)
	candidates := []string{
		filepath.Join(cfgDir, ".env.prod"),
		filepath.Join(cfgDir, ".env"),
		".env.prod",
		".env",
	}
	for _, f := range candidates {
		if _, err := os.Stat(f); err == nil {
			if err := godotenv.Load(f); err == nil {
				fmt.Fprintf(os.Stderr, "loaded env from %s\n", f)
				return
			}
		}
	}
}

func Execute() error {
	return rootCmd.Execute()
}