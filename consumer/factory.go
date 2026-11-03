// factory.go — CDC consumer factory.
//
// Implements scyllacdc.ChangeConsumerFactory interface required by scylla-cdc-go.
//
// How scylla-cdc-go works:
//   1. Each ScyllaDB CDC table has N streams (one per vnode/token range)
//   2. scylla-cdc-go creates one Reader per table
//   3. The Reader calls Factory.CreateChangeConsumer() for EACH stream
//   4. Each Consumer handles CDC events for one stream of one table
//   5. The Consumer calls writer.Add() to send rows to ClickHouse
//
// So for a table with 256 vnodes, there will be 256 Consumer instances,
// all sharing the same ClickHouseWriter (which is thread-safe via channels).
//
// The Factory also sets up PeriodicProgressReporter — this saves the CDC
// read position to ScyllaDB every checkpoint_interval (default 60s).
// On restart, scylla2ch resumes from the last checkpoint, not from scratch.
package consumer

import (
	"context"
	"log"
	"strings"

	scyllacdc "github.com/scylladb/scylla-cdc-go"

	"scylla2ch/config"
	"scylla2ch/metrics"
	"scylla2ch/writer"
)

// Factory creates a ChangeConsumer per CDC stream.
// Implements scyllacdc.ChangeConsumerFactory.
type Factory struct {
	cfg      *config.Config
	writer   *writer.ClickHouseWriter
	logger   *log.Logger
	chDBMap  map[string]string // ScyllaDB keyspace → ClickHouse database name
}

func NewFactory(cfg *config.Config, w *writer.ClickHouseWriter, l *log.Logger) *Factory {
	return &Factory{cfg: cfg, writer: w, logger: l, chDBMap: make(map[string]string)}
}

// SetKeyspaceDB maps a ScyllaDB keyspace to a ClickHouse database name.
// Used by multi-keyspace mode to route each keyspace's data to the right CH database.
func (f *Factory) SetKeyspaceDB(keyspace, chDB string) {
	f.chDBMap[keyspace] = chDB
}

func (f *Factory) CreateChangeConsumer(
	ctx context.Context,
	input scyllacdc.CreateChangeConsumerInput,
) (scyllacdc.ChangeConsumer, error) {
	tableName := input.TableName // e.g., "chat.rooms"

	// Use the built-in PeriodicProgressReporter for checkpointing
	reporter := scyllacdc.NewPeriodicProgressReporter(
		f.logger,
		f.cfg.CDC.CheckpointInterval, // default 60s
		input.ProgressReporter,       // provided by the reader
	)
	reporter.Start(ctx)

	f.logger.Printf("created consumer for table=%s stream=%v", tableName, input.StreamID)
	metrics.ConsumersActive.Inc()

	// Cache short table name and determine ClickHouse database
	shortName := tableName
	keyspace := ""
	if idx := strings.LastIndex(tableName, "."); idx != -1 {
		keyspace = tableName[:idx]
		shortName = tableName[idx+1:]
	}

	// Look up ClickHouse database for this keyspace (multi-keyspace support)
	chDB := f.chDBMap[keyspace]
	if chDB == "" {
		chDB = f.cfg.ClickHouse.Database // fallback for single-keyspace
	}

	return &Consumer{
		tableName: tableName,
		shortName: shortName,
		chDB:      chDB,
		cfg:       f.cfg,
		writer:    f.writer,
		logger:    f.logger,
		reporter:  reporter,
	}, nil
}
