// scylla2ch — Direct ScyllaDB CDC → ClickHouse replicator.
//
// Binary entrypoint. Delegates to cmd.Execute() which dispatches subcommands:
//   - run:      Start CDC streaming (main loop)
//   - snapshot: Bulk-load an existing ScyllaDB table into ClickHouse
//   - validate: Test connectivity to ScyllaDB Cloud + ClickHouse Cloud
//
// Architecture:
//
//	ScyllaDB CDC log tables  →  scylla-cdc-go reader  →  Consumer (type conversion)
//	  →  ClickHouseWriter (channel-per-table, batch flush)  →  ClickHouse
//
// See docs/architecture.md for full flow diagrams and comparison with alternatives.
package main

import (
	"fmt"
	"os"

	"scylla2ch/cmd"
)

func main() {
	if err := cmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
