package ingest

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

const (
	DefaultBatchSize       = 50000           // Records per partition before flush
	DefaultPartitionMaxAge = 5 * time.Second // Flush partition if no writes for this long
	DefaultCheckInterval   = 1 * time.Second // How often to check for stale partitions
)

// partitionBuffer holds records for a single partition
type partitionBuffer struct {
	records   []Record
	lastWrite time.Time
}

// BatchWriter writes records to ClickHouse in batches, grouped by partition
type BatchWriter struct {
	conn          driver.Conn
	batchSize     int
	partitionAge  time.Duration
	checkInterval time.Duration
	logger        *slog.Logger
	metrics       *Metrics

	// Per-partition buffers
	partitions map[int]*partitionBuffer
	mu         sync.Mutex
	done       chan struct{}
	wg         sync.WaitGroup

	// Semaphore to limit concurrent inserts
	insertSem chan struct{}

	// Throughput tracking
	recordsWritten  uint64
	statsMu         sync.Mutex
	lastStatsTime   time.Time
	lastRecordCount uint64

	// Backpressure tracking
	partCount     uint64
	partCountTime time.Time
	partCountMu   sync.RWMutex
}

// NewBatchWriter creates a new batch writer
func NewBatchWriter(conn driver.Conn, logger *slog.Logger, metrics *Metrics) *BatchWriter {
	w := &BatchWriter{
		conn:          conn,
		batchSize:     DefaultBatchSize,
		partitionAge:  DefaultPartitionMaxAge,
		checkInterval: DefaultCheckInterval,
		logger:        logger,
		metrics:       metrics,
		partitions:    make(map[int]*partitionBuffer),
		done:          make(chan struct{}),
		insertSem:     make(chan struct{}, 30), // Limit concurrent inserts
		lastStatsTime: time.Now(),
	}

	w.wg.Add(2)
	go w.flushLoop()
	go w.statsLoop()

	return w
}

// statsLoop logs throughput every 10 seconds
func (w *BatchWriter) statsLoop() {
	defer w.wg.Done()
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			w.statsMu.Lock()
			now := time.Now()
			elapsed := now.Sub(w.lastStatsTime).Seconds()
			currentRecords := w.recordsWritten
			recordsDiff := currentRecords - w.lastRecordCount
			rps := float64(recordsDiff) / elapsed

			w.logger.Info("throughput",
				"records_total", currentRecords,
				"records_per_sec", fmt.Sprintf("%.1f", rps),
			)

			w.lastStatsTime = now
			w.lastRecordCount = currentRecords
			w.statsMu.Unlock()
		case <-w.done:
			return
		}
	}
}

// partitionKey returns the partition key for a record (matches ClickHouse PARTITION BY)
func partitionKey(t time.Time) int {
	if t.Year() < 2023 {
		return 202200
	}
	return t.Year()*100 + int(t.Month())
}

// WriteRecord adds a record to the appropriate partition buffer
func (w *BatchWriter) WriteRecord(r Record) {
	var toFlush []Record

	w.mu.Lock()
	pk := partitionKey(r.CreatedAt)
	buf, ok := w.partitions[pk]
	if !ok {
		buf = &partitionBuffer{
			records: make([]Record, 0, w.batchSize),
		}
		w.partitions[pk] = buf
	}

	buf.records = append(buf.records, r)
	buf.lastWrite = r.IndexedAt

	if len(buf.records) >= w.batchSize {
		toFlush = buf.records
		buf.records = make([]Record, 0, w.batchSize)
	}
	w.mu.Unlock()

	if toFlush != nil {
		w.dispatchInsert(toFlush)
	}
}

// flushLoop periodically checks for stale partitions
func (w *BatchWriter) flushLoop() {
	defer w.wg.Done()
	ticker := time.NewTicker(w.checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			w.flushStalePartitions()
		case <-w.done:
			w.FlushAll()
			return
		}
	}
}

// flushStalePartitions flushes partitions that haven't been written to recently
func (w *BatchWriter) flushStalePartitions() {
	var toFlush [][]Record

	w.mu.Lock()
	now := time.Now()
	for pk, buf := range w.partitions {
		if len(buf.records) > 0 && now.Sub(buf.lastWrite) > w.partitionAge {
			toFlush = append(toFlush, buf.records)
			buf.records = make([]Record, 0, w.batchSize)
			_ = pk
		}
	}
	w.mu.Unlock()

	for _, records := range toFlush {
		w.dispatchInsert(records)
	}
}

// FlushAll flushes all pending records (used for shutdown)
func (w *BatchWriter) FlushAll() {
	var toFlush [][]Record

	w.mu.Lock()
	for pk, buf := range w.partitions {
		if len(buf.records) > 0 {
			toFlush = append(toFlush, buf.records)
			buf.records = make([]Record, 0, w.batchSize)
			_ = pk
		}
	}
	w.mu.Unlock()

	for _, records := range toFlush {
		w.dispatchInsert(records)
	}
}

// dispatchInsert acquires the semaphore and sends a batch for insertion.
// Must be called WITHOUT holding w.mu.
func (w *BatchWriter) dispatchInsert(records []Record) {
	w.insertSem <- struct{}{}
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		defer func() { <-w.insertSem }()
		w.insertRecords(records)
	}()
}

// getPartCount returns the cached part count, refreshing if stale
func (w *BatchWriter) getPartCount(ctx context.Context) uint64 {
	const cacheAge = 1 * time.Second

	// Check if cached value is fresh enough
	w.partCountMu.RLock()
	if time.Since(w.partCountTime) < cacheAge {
		count := w.partCount
		w.partCountMu.RUnlock()
		return count
	}
	w.partCountMu.RUnlock()

	// Need to refresh - acquire write lock
	w.partCountMu.Lock()
	defer w.partCountMu.Unlock()

	// Double-check in case another goroutine refreshed while we waited
	if time.Since(w.partCountTime) < cacheAge {
		return w.partCount
	}

	var partCount uint64
	err := w.conn.QueryRow(ctx, `
		SELECT count() FROM system.parts
		WHERE database = 'attie' AND table = 'records' AND active
	`).Scan(&partCount)
	if err != nil {
		w.logger.Warn("failed to check part count", "error", err)
		return w.partCount // Return stale value on error
	}

	w.partCount = partCount
	w.partCountTime = time.Now()
	return partCount
}

// waitForMerges blocks until the part count is below the threshold
func (w *BatchWriter) waitForMerges(ctx context.Context) {
	const (
		partThreshold = 3000 // Start waiting when parts exceed this
		checkInterval = 500 * time.Millisecond
	)

	for {
		partCount := w.getPartCount(ctx)
		if partCount < partThreshold {
			return
		}

		w.logger.Info("waiting for merges", "parts", partCount, "threshold", partThreshold)
		select {
		case <-ctx.Done():
			return
		case <-time.After(checkInterval):
			// Invalidate cache so next getPartCount actually queries
			w.partCountMu.Lock()
			w.partCountTime = time.Time{}
			w.partCountMu.Unlock()
		}
	}
}

// insertRecords inserts records into ClickHouse with retries.
// Caller must have already acquired insertSem.
func (w *BatchWriter) insertRecords(records []Record) {
	// Wait for merges if part count is too high
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	w.waitForMerges(ctx)

	maxRetries := 12
	baseDelay := 500 * time.Millisecond
	maxDelay := 30 * time.Second

	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			delay := baseDelay * time.Duration(1<<uint(attempt-1)) // exponential backoff
			if delay > maxDelay {
				delay = maxDelay
			}
			time.Sleep(delay)
		}

		err := w.doInsertRecords(records)
		if err == nil {
			return
		}

		w.logger.Warn("insert records failed, retrying", "error", err, "attempt", attempt+1, "count", len(records))
	}

	w.metrics.RecordsFailed.Add(context.Background(), int64(len(records)))
	w.logger.Error("insert records failed after retries, DATA LOST", "count", len(records))
}

func (w *BatchWriter) doInsertRecords(records []Record) error {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	batch, err := w.conn.PrepareBatch(ctx, `
		INSERT INTO attie.records (uri, did, collection, rkey, record, created_at, indexed_at, seq)
	`)
	if err != nil {
		return fmt.Errorf("prepare batch: %w", err)
	}

	for _, r := range records {
		err := batch.Append(
			r.URI,
			r.DID,
			r.Collection,
			r.Rkey,
			r.Record,
			r.CreatedAt,
			r.IndexedAt,
			r.Seq,
		)
		if err != nil {
			w.logger.Error("append record", "error", err, "uri", r.URI)
		}
	}

	if err := batch.Send(); err != nil {
		return fmt.Errorf("send batch: %w", err)
	}

	n := int64(len(records))
	w.statsMu.Lock()
	w.recordsWritten += uint64(n)
	w.statsMu.Unlock()

	w.metrics.RecordsWritten.Add(context.Background(), n)
	w.metrics.BatchFlushes.Add(context.Background(), 1)
	w.metrics.BatchSize.Record(context.Background(), n)

	w.logger.Debug("flushed records", "count", n)
	return nil
}

// Close stops the writer and flushes remaining records.
// Waits for all in-flight inserts to complete to avoid data loss.
func (w *BatchWriter) Close() error {
	close(w.done)
	w.logger.Info("record writer draining in-flight inserts...")
	w.wg.Wait()
	w.logger.Info("record writer closed successfully")
	return nil
}
