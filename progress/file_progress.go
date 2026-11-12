// Package progress provides a file-backed CDC checkpoint manager.
//
// Instead of writing checkpoints to ScyllaDB (which adds write load to the production
// cluster), this stores one progress entry per table in a local JSON file on EBS.
//
// Design:
//   - In-memory: per-table progress (max timeuuid across all streams)
//   - On disk: periodic JSON dump (default: every 1 hour)
//   - On shutdown: final flush (graceful drain)
//   - On restart: load from file → resume from checkpoint. No file → use ChangeAgeLimit.
//
// Why per-table (not per-stream):
//   - One entry per table = tiny file (10 tables ≈ 1 KB)
//   - On restart, all streams resume from the table-level checkpoint
//   - ClickHouse ReplacingMergeTree(_cdc_ts) deduplicates any replayed rows
//   - Max replay = checkpoint_interval (default 1h) — negligible with dedup
//
// Recovery scenarios:
//   - Process restart:  read file → replay ≤1h → dedup in ClickHouse ✓
//   - EC2 reboot:       EBS survives → same as restart ✓
//   - EC2 terminated:   no file → use change_age_limit (8h) → dedup ✓
package progress

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/gocql/gocql"
	scyllacdc "github.com/scylladb/scylla-cdc-go"
)

// FileProgressManager saves CDC checkpoints to a local JSON file instead of ScyllaDB.
// Implements both scyllacdc.ProgressManager and scyllacdc.ProgressManagerWithStartTime.
type FileProgressManager struct {
	mu           sync.RWMutex
	generation   time.Time
	appStartTime time.Time
	// Per-table progress: table_name ("keyspace.table") → latest timeuuid
	// Stores the max across all streams — on restart all streams resume from here.
	tableProgress map[string]gocql.UUID

	filePath      string
	flushInterval time.Duration
	logger        *log.Logger
	dirty         bool // true when in-memory state has unflushed changes
	stopCh        chan struct{}
	doneCh        chan struct{}
}

// fileCheckpoint is the JSON structure written to disk.
type fileCheckpoint struct {
	Version      int                        `json:"version"`
	UpdatedAt    time.Time                  `json:"updated_at"`
	Generation   string                     `json:"generation,omitempty"`
	AppStartTime string                     `json:"app_start_time,omitempty"`
	Tables       map[string]tableCheckpoint `json:"tables"`
}

// tableCheckpoint is one entry per table in the JSON file.
type tableCheckpoint struct {
	LastProgress string    `json:"last_progress"` // timeuuid string
	UpdatedAt    time.Time `json:"updated_at"`
}

// NewFileProgressManager creates a file-backed progress manager.
// Loads existing checkpoint from disk (if any) and starts the periodic flush goroutine.
func NewFileProgressManager(filePath string, flushInterval time.Duration, logger *log.Logger) (*FileProgressManager, error) {
	fpm := &FileProgressManager{
		tableProgress: make(map[string]gocql.UUID),
		filePath:      filePath,
		flushInterval: flushInterval,
		logger:        logger,
		stopCh:        make(chan struct{}),
		doneCh:        make(chan struct{}),
	}

	// Load existing checkpoint file
	if err := fpm.loadFromDisk(); err != nil {
		logger.Printf("checkpoint: file %s not found or invalid (%v) — starting fresh", filePath, err)
	}

	// Start periodic flush goroutine
	go fpm.flushLoop()

	return fpm, nil
}

func (f *FileProgressManager) loadFromDisk() error {
	data, err := os.ReadFile(f.filePath)
	if err != nil {
		return err
	}

	var cp fileCheckpoint
	if err := json.Unmarshal(data, &cp); err != nil {
		return fmt.Errorf("parse checkpoint: %w", err)
	}

	if cp.Generation != "" {
		gen, err := time.Parse(time.RFC3339Nano, cp.Generation)
		if err == nil {
			f.generation = gen
		}
	}

	if cp.AppStartTime != "" {
		ast, err := time.Parse(time.RFC3339Nano, cp.AppStartTime)
		if err == nil {
			f.appStartTime = ast
		}
	}

	for table, tc := range cp.Tables {
		if tc.LastProgress != "" {
			uid, err := gocql.ParseUUID(tc.LastProgress)
			if err == nil {
				f.tableProgress[table] = uid
				f.logger.Printf("checkpoint: loaded table=%s progress=%s (%s)", table, uid, uid.Time())
			}
		}
	}

	f.logger.Printf("checkpoint: loaded %d tables from %s (generation=%v)", len(f.tableProgress), f.filePath, f.generation)
	return nil
}

func (f *FileProgressManager) flushToDisk() error {
	f.mu.RLock()
	cp := fileCheckpoint{
		Version:   1,
		UpdatedAt: time.Now(),
		Tables:    make(map[string]tableCheckpoint, len(f.tableProgress)),
	}
	if !f.generation.IsZero() {
		cp.Generation = f.generation.Format(time.RFC3339Nano)
	}
	if !f.appStartTime.IsZero() {
		cp.AppStartTime = f.appStartTime.Format(time.RFC3339Nano)
	}
	for table, uid := range f.tableProgress {
		cp.Tables[table] = tableCheckpoint{
			LastProgress: uid.String(),
			UpdatedAt:    time.Now(),
		}
	}
	f.mu.RUnlock()

	data, err := json.MarshalIndent(cp, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal checkpoint: %w", err)
	}

	// Atomic write: temp file + rename — no partial reads on crash
	tmpFile := f.filePath + ".tmp"
	if err := os.WriteFile(tmpFile, data, 0644); err != nil {
		return fmt.Errorf("write checkpoint: %w", err)
	}
	if err := os.Rename(tmpFile, f.filePath); err != nil {
		return fmt.Errorf("rename checkpoint: %w", err)
	}

	return nil
}

func (f *FileProgressManager) flushLoop() {
	defer close(f.doneCh)
	ticker := time.NewTicker(f.flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			f.mu.RLock()
			dirty := f.dirty
			f.mu.RUnlock()
			if dirty {
				if err := f.flushToDisk(); err != nil {
					f.logger.Printf("ERROR checkpoint flush: %v", err)
				} else {
					f.mu.Lock()
					f.dirty = false
					f.mu.Unlock()
					f.logger.Printf("checkpoint: flushed %d tables to %s", len(f.tableProgress), f.filePath)
				}
			}
		case <-f.stopCh:
			// Final flush on shutdown — don't lose progress
			if err := f.flushToDisk(); err != nil {
				f.logger.Printf("ERROR checkpoint final flush: %v", err)
			} else {
				f.logger.Printf("checkpoint: final flush to %s", f.filePath)
			}
			return
		}
	}
}

// Stop flushes the checkpoint to disk and stops the background goroutine.
// Called during graceful shutdown.
func (f *FileProgressManager) Stop() {
	close(f.stopCh)
	<-f.doneCh
}

// ClearTable removes a table's checkpoint from memory.
// Used by dashboard "fresh start" — next reader start uses ChangeAgeLimit.
func (f *FileProgressManager) ClearTable(table string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.tableProgress, table)
	f.dirty = true
	f.logger.Printf("checkpoint: cleared table=%s (fresh start)", table)
}

// TableCount returns the number of tables with checkpoints.
func (f *FileProgressManager) TableCount() int {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return len(f.tableProgress)
}

// --- scyllacdc.ProgressManager interface ---

func (f *FileProgressManager) GetCurrentGeneration(ctx context.Context) (time.Time, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.generation, nil
}

func (f *FileProgressManager) StartGeneration(ctx context.Context, gen time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.generation = gen
	f.dirty = true
	return nil
}

func (f *FileProgressManager) GetProgress(ctx context.Context, gen time.Time, table string, streamID scyllacdc.StreamID) (scyllacdc.Progress, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if uid, ok := f.tableProgress[table]; ok {
		return scyllacdc.Progress{LastProcessedRecordTime: uid}, nil
	}
	return scyllacdc.Progress{}, nil
}

func (f *FileProgressManager) SaveProgress(ctx context.Context, gen time.Time, table string, streamID scyllacdc.StreamID, p scyllacdc.Progress) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Keep the maximum progress across all streams for this table.
	// On restart, all streams resume from here — slight over-replay is handled by dedup.
	existing, ok := f.tableProgress[table]
	if !ok || scyllacdc.CompareTimeUUID(p.LastProcessedRecordTime, existing) > 0 {
		f.tableProgress[table] = p.LastProcessedRecordTime
		f.dirty = true
	}
	return nil
}

// --- scyllacdc.ProgressManagerWithStartTime interface ---

func (f *FileProgressManager) GetApplicationReadStartTime(ctx context.Context) (time.Time, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.appStartTime, nil
}

func (f *FileProgressManager) SaveApplicationReadStartTime(ctx context.Context, startTime time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.appStartTime = startTime
	f.dirty = true
	return nil
}