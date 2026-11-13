// Package progress provides a file-backed CDC checkpoint manager.
//
// Instead of writing checkpoints to ScyllaDB (which adds write load to the production
// cluster), this stores one progress entry per table in a local JSON file on EBS.
//
// Design:
//   - In-memory: per-stream progress (timeuuid per table+stream pair)
//   - Per-table fallback: MIN of all streams (backward compat for old checkpoint files)
//   - On disk: periodic JSON dump (default: every 1 minute)
//   - On shutdown: final flush (graceful drain)
//   - On restart: load from file → resume from checkpoint. No file → use ChangeAgeLimit.
//
// FIX #13: Per-stream checkpoint replaces the old MAX-across-streams approach.
// The old approach kept MAX timeuuid across all streams for a table. On restart,
// slower streams would skip rows because they'd resume from the faster stream's
// checkpoint. Now each stream has its own checkpoint. The table-level value is
// MIN of all streams (safe resume point for backward compat / fast fallback).
//
// Recovery scenarios:
//   - Process restart:  read file → each stream resumes from its own checkpoint ✓
//   - EC2 reboot:       EBS survives → same as restart ✓
//   - EC2 terminated:   no file → use change_age_limit (8h) → dedup ✓
//
// TODO (#7 HA): For production HA, move to ScyllaDB-backed progress (TableBackedProgressManager)
// or shared EFS/S3 checkpoint. Local file means single-instance only — no failover.
// TODO (#8 Delta mode): Add config option `cdc.mode: "postimage"` to use PostImage instead
// of Delta for tables with frequent UPDATEs (avoids NULL columns in ClickHouse).
package progress

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/gocql/gocql"
	scyllacdc "github.com/scylladb/scylla-cdc-go"
)

// streamKey identifies a unique CDC stream within a table.
type streamKey struct {
	Table    string
	StreamID string // hex of scyllacdc.StreamID
}

// FileProgressManager saves CDC checkpoints to a local JSON file instead of ScyllaDB.
// Implements both scyllacdc.ProgressManager and scyllacdc.ProgressManagerWithStartTime.
type FileProgressManager struct {
	mu           sync.RWMutex
	generation   time.Time
	appStartTime time.Time
	// Per-table progress: table_name ("keyspace.table") → MIN timeuuid across all streams.
	// Used as fallback when per-stream data is not available (backward compat).
	tableProgress map[string]gocql.UUID
	// FIX #13: Per-stream progress — each stream has its own checkpoint.
	// Prevents restart from skipping rows of slower streams.
	streamProgress map[streamKey]gocql.UUID

	// blocked tables: SaveProgress is a no-op for these tables.
	// Prevents race: old reader's End() → SaveProgress writes back old progress
	// AFTER ClearTable removed it. Without this, remove+re-add has a race window
	// where the old reader's final checkpoint overwrites the cleared state.
	blocked sync.Map // table name → struct{}

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
	// FIX #13: Per-stream progress. Outer key = table name, inner key = stream hex, value = timeuuid string.
	// Old checkpoint files without this field still load fine (backward compat).
	Streams map[string]map[string]string `json:"streams,omitempty"`
}

// tableCheckpoint is one entry per table in the JSON file.
type tableCheckpoint struct {
	LastProgress string    `json:"last_progress"` // timeuuid string (MIN of all streams)
	UpdatedAt    time.Time `json:"updated_at"`
}

// NewFileProgressManager creates a file-backed progress manager.
// Loads existing checkpoint from disk (if any) and starts the periodic flush goroutine.
func NewFileProgressManager(filePath string, flushInterval time.Duration, logger *log.Logger) (*FileProgressManager, error) {
	fpm := &FileProgressManager{
		tableProgress:  make(map[string]gocql.UUID),
		streamProgress: make(map[streamKey]gocql.UUID),
		filePath:       filePath,
		flushInterval:  flushInterval,
		logger:         logger,
		stopCh:         make(chan struct{}),
		doneCh:         make(chan struct{}),
	}

	// Load existing checkpoint file
	if err := fpm.loadFromDisk(); err != nil {
		if os.IsNotExist(err) {
			logger.Printf("checkpoint: file %s not found — starting fresh", filePath)
		} else {
			// FIX #9: Corrupt file is a real error — don't silently start fresh.
			// Returning error lets caller decide (log + continue or abort).
			return nil, fmt.Errorf("checkpoint load %s: %w", filePath, err)
		}
	}

	// Start periodic flush goroutine
	go fpm.flushLoop()

	return fpm, nil
}

func (f *FileProgressManager) loadFromDisk() error {
	data, err := os.ReadFile(f.filePath)
	if err != nil {
		return err // os.IsNotExist checked by caller
	}

	var cp fileCheckpoint
	if err := json.Unmarshal(data, &cp); err != nil {
		// FIX #9: Corrupt file — return error instead of silently starting fresh.
		return fmt.Errorf("parse checkpoint (corrupt file?): %w", err)
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

	// FIX #13: Load per-stream progress (backward compat — old files have no streams key)
	streamCount := 0
	for table, streams := range cp.Streams {
		for streamHex, uidStr := range streams {
			uid, err := gocql.ParseUUID(uidStr)
			if err == nil {
				f.streamProgress[streamKey{Table: table, StreamID: streamHex}] = uid
				streamCount++
			}
		}
	}

	f.logger.Printf("checkpoint: loaded %d tables, %d streams from %s (generation=%v)", len(f.tableProgress), streamCount, f.filePath, f.generation)
	return nil
}

func (f *FileProgressManager) flushToDisk() error {
	f.mu.RLock()
	cp := fileCheckpoint{
		Version:   2, // v2 = per-stream checkpoint
		UpdatedAt: time.Now(),
		Tables:    make(map[string]tableCheckpoint, len(f.tableProgress)),
		Streams:   make(map[string]map[string]string),
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
	// FIX #13: Write per-stream progress
	for sk, uid := range f.streamProgress {
		if _, ok := cp.Streams[sk.Table]; !ok {
			cp.Streams[sk.Table] = make(map[string]string)
		}
		cp.Streams[sk.Table][sk.StreamID] = uid.String()
	}
	f.mu.RUnlock()

	data, err := json.MarshalIndent(cp, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal checkpoint: %w", err)
	}

	// FIX #9: Atomic write with fsync + unique tmp name + .bak
	// Unique tmp prevents race during tableflip (two processes flushing simultaneously)
	tmpFile := f.filePath + ".tmp." + strconv.Itoa(os.Getpid())

	tmpF, err := os.OpenFile(tmpFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("create checkpoint tmp: %w", err)
	}
	if _, err := tmpF.Write(data); err != nil {
		tmpF.Close()
		os.Remove(tmpFile)
		return fmt.Errorf("write checkpoint tmp: %w", err)
	}
	// fsync the file — ensures data is on disk before rename
	if err := tmpF.Sync(); err != nil {
		tmpF.Close()
		os.Remove(tmpFile)
		return fmt.Errorf("fsync checkpoint tmp: %w", err)
	}
	if err := tmpF.Close(); err != nil {
		os.Remove(tmpFile)
		return fmt.Errorf("close checkpoint tmp: %w", err)
	}

	// Keep .bak of previous checkpoint for recovery
	if _, err := os.Stat(f.filePath); err == nil {
		_ = os.Rename(f.filePath, f.filePath+".bak")
	}

	if err := os.Rename(tmpFile, f.filePath); err != nil {
		return fmt.Errorf("rename checkpoint: %w", err)
	}

	// fsync the directory — ensures the rename is durable
	if dir, err := os.Open(filepath.Dir(f.filePath)); err == nil {
		_ = dir.Sync()
		dir.Close()
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

// ClearTable removes a table's checkpoint from memory, blocks stale SaveProgress calls
// from the old reader's End(), and immediately flushes to disk so the cleared state
// survives process restart (SIGUSR2 / systemctl restart).
//
// The block prevents a race: stopTable → cancelFn() (async) → old reader End() →
// SaveProgress writes old progress back AFTER ClearTable deleted it.
// Call UnblockTable when the new reader is about to start.
func (f *FileProgressManager) ClearTable(table string) {
	// Block SaveProgress for this table FIRST — prevents race with old reader's End()
	f.blocked.Store(table, struct{}{})

	f.mu.Lock()
	delete(f.tableProgress, table)
	// FIX #13: Delete all stream entries for the table
	for sk := range f.streamProgress {
		if sk.Table == table {
			delete(f.streamProgress, sk)
		}
	}
	// Reset global timestamps so scylla-cdc-go's determineStartTimestamp falls through
	// to ChangeAgeLimit. Without this, the old appStartTime/generation (set by the
	// first-ever reader, possibly 8h ago) is returned by GetApplicationReadStartTime /
	// GetCurrentGeneration, and the library starts from there — ignoring ChangeAgeLimit.
	// Running readers are unaffected (they don't re-call determineStartTimestamp).
	// On restart, tables with per-stream progress resume from there regardless.
	f.generation = time.Time{}
	f.appStartTime = time.Time{}
	f.dirty = true
	f.mu.Unlock()

	// Immediately flush to disk — ensures cleared state survives process restart.
	// Without this, SIGUSR2 loads the old checkpoint from disk and resumes from 8h ago.
	if err := f.flushToDisk(); err != nil {
		f.logger.Printf("ERROR checkpoint: flush after clear table=%s: %v", table, err)
	} else {
		f.logger.Printf("checkpoint: cleared + flushed table=%s (fresh start persisted to disk)", table)
	}
}

// UnblockTable re-enables SaveProgress for a table after ClearTable blocked it.
// Called when the new reader is set up and ready to save its own progress.
func (f *FileProgressManager) UnblockTable(table string) {
	f.blocked.Delete(table)
}

// TableCount returns the number of tables with checkpoints.
func (f *FileProgressManager) TableCount() int {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return len(f.tableProgress)
}

// StreamCount returns the number of per-stream checkpoints.
// Used by the dashboard to show checkpoint granularity.
func (f *FileProgressManager) StreamCount() int {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return len(f.streamProgress)
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
	// FIX #13: Return per-stream value if available, else fall back to table-level.
	// Table-level fallback causes a replay (safe — dedup handles it).
	sk := streamKey{Table: table, StreamID: hex.EncodeToString(streamID)}
	if uid, ok := f.streamProgress[sk]; ok {
		return scyllacdc.Progress{LastProcessedRecordTime: uid}, nil
	}
	if uid, ok := f.tableProgress[table]; ok {
		return scyllacdc.Progress{LastProcessedRecordTime: uid}, nil
	}
	return scyllacdc.Progress{}, nil
}

func (f *FileProgressManager) SaveProgress(ctx context.Context, gen time.Time, table string, streamID scyllacdc.StreamID, p scyllacdc.Progress) error {
	// Check if this table is blocked (being cleared for fresh start).
	// Old reader's End() → SaveProgress can race with ClearTable — block prevents it.
	if _, blocked := f.blocked.Load(table); blocked {
		return nil
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	// FIX #13: Store per-stream progress AND update table-level to MIN of all streams.
	sk := streamKey{Table: table, StreamID: hex.EncodeToString(streamID)}
	existing, ok := f.streamProgress[sk]
	if !ok || scyllacdc.CompareTimeUUID(p.LastProcessedRecordTime, existing) > 0 {
		f.streamProgress[sk] = p.LastProcessedRecordTime
		f.dirty = true
	}

	// Recompute table-level progress as MIN of all streams for this table.
	// This is the safe resume point for backward compat / new streams without history.
	var minUID gocql.UUID
	minSet := false
	for k, uid := range f.streamProgress {
		if k.Table == table {
			if !minSet || scyllacdc.CompareTimeUUID(uid, minUID) < 0 {
				minUID = uid
				minSet = true
			}
		}
	}
	if minSet {
		f.tableProgress[table] = minUID
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
