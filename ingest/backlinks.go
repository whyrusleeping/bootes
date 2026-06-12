package ingest

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// BacklinkRecord is the full record for ClickHouse insertion
type BacklinkRecord struct {
	Ref        string    // The DID or AT-URI being referenced
	RefType    string    // "did" or "uri"
	Collection string    // Collection of the source record
	Path       string    // JSON path where ref was found
	SourceURI  string    // AT-URI of the record containing this reference
	SourceDID  string    // DID of the record author
	CreatedAt  time.Time // When the source record was created
}

// backlinkPartitionBuffer holds records for a single partition
type backlinkPartitionBuffer struct {
	records   []BacklinkRecord
	lastWrite time.Time
}

// BacklinkWriter handles batched backlink insertion to ClickHouse,
// grouping records by partition to avoid creating multiple parts per insert.
type BacklinkWriter struct {
	conn          driver.Conn
	logger        *slog.Logger
	metrics       *Metrics
	batchSize     int
	partitionAge  time.Duration
	checkInterval time.Duration
	done          chan struct{}
	wg            sync.WaitGroup
	sem           chan struct{}

	// Per-partition buffers
	partitions map[int]*backlinkPartitionBuffer
	mu         sync.Mutex

	// Stats
	written uint64
	statsMu sync.Mutex
}

// NewBacklinkWriter creates a new BacklinkWriter
func NewBacklinkWriter(conn driver.Conn, logger *slog.Logger, metrics *Metrics) *BacklinkWriter {
	w := &BacklinkWriter{
		conn:          conn,
		logger:        logger,
		metrics:       metrics,
		batchSize:     10000,
		partitionAge:  5 * time.Second,
		checkInterval: 1 * time.Second,
		done:          make(chan struct{}),
		sem:           make(chan struct{}, 8),
		partitions:    make(map[int]*backlinkPartitionBuffer),
	}

	w.wg.Add(1)
	go w.flushLoop()

	return w
}

// backlinkPartitionKey returns the partition key matching the ClickHouse PARTITION BY toYYYYMM(created_at)
func backlinkPartitionKey(t time.Time) int {
	return t.Year()*100 + int(t.Month())
}

// Write adds backlink records to the appropriate partition buffers
func (w *BacklinkWriter) Write(backlinks []BacklinkRecord) {
	if len(backlinks) == 0 {
		return
	}

	// Pre-sort backlinks by partition outside the lock
	byPartition := make(map[int][]BacklinkRecord)
	for i := range backlinks {
		pk := backlinkPartitionKey(backlinks[i].CreatedAt)
		byPartition[pk] = append(byPartition[pk], backlinks[i])
	}

	var toFlush [][]BacklinkRecord
	now := time.Now()

	w.mu.Lock()
	for pk, records := range byPartition {
		buf, ok := w.partitions[pk]
		if !ok {
			buf = &backlinkPartitionBuffer{
				records: make([]BacklinkRecord, 0, w.batchSize),
			}
			w.partitions[pk] = buf
		}

		buf.records = append(buf.records, records...)
		buf.lastWrite = now

		for len(buf.records) >= w.batchSize {
			toFlush = append(toFlush, buf.records[:w.batchSize])
			remaining := make([]BacklinkRecord, len(buf.records)-w.batchSize)
			copy(remaining, buf.records[w.batchSize:])
			buf.records = remaining
		}
	}
	w.mu.Unlock()

	// Dispatch flushes outside the lock
	for _, records := range toFlush {
		w.dispatchInsert(records)
	}
}

// flushLoop periodically flushes stale partitions
func (w *BacklinkWriter) flushLoop() {
	defer w.wg.Done()
	ticker := time.NewTicker(w.checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			w.flushStalePartitions()
		case <-w.done:
			w.flushAll()
			return
		}
	}
}

// flushStalePartitions flushes partitions that haven't been written to recently
func (w *BacklinkWriter) flushStalePartitions() {
	var toFlush [][]BacklinkRecord

	w.mu.Lock()
	now := time.Now()
	for pk, buf := range w.partitions {
		if len(buf.records) > 0 && now.Sub(buf.lastWrite) > w.partitionAge {
			if records := w.takePartitionLocked(pk); records != nil {
				toFlush = append(toFlush, records)
			}
			_ = buf
		}
	}
	w.mu.Unlock()

	for _, records := range toFlush {
		w.dispatchInsert(records)
	}
}

// flushAll flushes all partitions (used for shutdown)
func (w *BacklinkWriter) flushAll() {
	var toFlush [][]BacklinkRecord

	w.mu.Lock()
	for pk := range w.partitions {
		if records := w.takePartitionLocked(pk); records != nil {
			toFlush = append(toFlush, records)
		}
	}
	w.mu.Unlock()

	for _, records := range toFlush {
		w.dispatchInsert(records)
	}
}

// dispatchInsert acquires the semaphore and sends a batch for insertion.
// Must be called WITHOUT holding w.mu.
func (w *BacklinkWriter) dispatchInsert(records []BacklinkRecord) {
	w.sem <- struct{}{}
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		defer func() { <-w.sem }()
		w.insertRecords(records)
	}()
}

// flushPartitionLocked extracts a partition's records (must hold lock).
// Returns the records to flush, or nil if nothing to flush.
func (w *BacklinkWriter) takePartitionLocked(pk int) []BacklinkRecord {
	buf, ok := w.partitions[pk]
	if !ok || len(buf.records) == 0 {
		return nil
	}
	records := buf.records
	buf.records = make([]BacklinkRecord, 0, w.batchSize)
	return records
}

// waitForMerges blocks until the backlinks part count is below the threshold
func (w *BacklinkWriter) waitForMerges(ctx context.Context) {
	const (
		partThreshold = 3000
		checkInterval = 2 * time.Second
	)

	for {
		var partCount uint64
		err := w.conn.QueryRow(ctx, `
			SELECT count() FROM system.parts
			WHERE database = 'attie' AND table = 'backlinks' AND active
		`).Scan(&partCount)
		if err != nil {
			w.logger.Warn("failed to check backlinks part count", "error", err)
			return
		}
		if partCount < partThreshold {
			return
		}

		w.logger.Info("backlinks: waiting for merges", "parts", partCount, "threshold", partThreshold)
		select {
		case <-ctx.Done():
			return
		case <-time.After(checkInterval):
		}
	}
}

// isTooManyParts checks if the error is a ClickHouse "too many parts" error
func isTooManyParts(err error) bool {
	return err != nil && strings.Contains(err.Error(), "Too many parts")
}

// insertRecords inserts records into ClickHouse with retries
func (w *BacklinkWriter) insertRecords(records []BacklinkRecord) {
	maxRetries := 12
	baseDelay := 500 * time.Millisecond
	maxDelay := 30 * time.Second

	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			delay := baseDelay * time.Duration(1<<uint(attempt-1))
			if delay > maxDelay {
				delay = maxDelay
			}
			time.Sleep(delay)
		}

		err := w.doInsertRecords(records)
		if err == nil {
			return
		}

		w.logger.Warn("insert backlinks failed, retrying", "error", err, "attempt", attempt+1, "count", len(records))

		if isTooManyParts(err) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			w.waitForMerges(ctx)
			cancel()
		}
	}

	w.logger.Error("insert backlinks failed after retries, DATA LOST", "count", len(records))
}

func (w *BacklinkWriter) doInsertRecords(records []BacklinkRecord) error {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	batch, err := w.conn.PrepareBatch(ctx, `
		INSERT INTO attie.backlinks (ref, ref_type, collection, path, source_uri, source_did, created_at)
	`)
	if err != nil {
		return fmt.Errorf("prepare batch: %w", err)
	}

	for _, r := range records {
		var refType int8
		if r.RefType == "uri" {
			refType = 2
		} else {
			refType = 1
		}

		err := batch.Append(
			r.Ref,
			refType,
			r.Collection,
			r.Path,
			r.SourceURI,
			r.SourceDID,
			r.CreatedAt,
		)
		if err != nil {
			w.logger.Error("append backlink", "error", err, "ref", r.Ref)
		}
	}

	if err := batch.Send(); err != nil {
		return fmt.Errorf("send batch: %w", err)
	}

	n := int64(len(records))
	w.statsMu.Lock()
	w.written += uint64(n)
	w.statsMu.Unlock()

	w.metrics.BacklinksWritten.Add(context.Background(), n)
	w.logger.Debug("flushed backlinks", "count", n, "partition", backlinkPartitionKey(records[0].CreatedAt))
	return nil
}

// Stats returns the number of backlinks written
func (w *BacklinkWriter) Stats() uint64 {
	w.statsMu.Lock()
	defer w.statsMu.Unlock()
	return w.written
}

// Close stops the writer and flushes remaining records.
// Waits for all in-flight inserts to complete to avoid data loss.
func (w *BacklinkWriter) Close() error {
	close(w.done)
	w.logger.Info("backlink writer draining in-flight inserts...")
	w.wg.Wait()
	w.logger.Info("backlink writer closed successfully", "total_written", w.written)
	return nil
}
