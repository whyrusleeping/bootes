package ingest

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// DeleteQueue handles deferred deletion by writing to pending_deletes table
type DeleteQueue struct {
	conn    driver.Conn
	buf     []pendingDelete
	mu      sync.Mutex
	logger  *slog.Logger
	metrics *Metrics

	// Cleanup job
	cleanupInterval time.Duration
	done            chan struct{}
	wg              sync.WaitGroup
}

type pendingDelete struct {
	uri       string
	tableName string
}

// NewDeleteQueue creates a new DeleteQueue
func NewDeleteQueue(conn driver.Conn, logger *slog.Logger, metrics *Metrics) *DeleteQueue {
	q := &DeleteQueue{
		conn:            conn,
		buf:             make([]pendingDelete, 0, 1000),
		logger:          logger,
		metrics:         metrics,
		cleanupInterval: 20 * time.Minute,
		done:            make(chan struct{}),
	}

	q.wg.Add(1)
	go q.flushLoop()

	q.wg.Add(1)
	go q.cleanupLoop()

	return q
}

// QueueRecordDelete queues a record URI for deletion
func (q *DeleteQueue) QueueRecordDelete(uri string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.buf = append(q.buf, pendingDelete{uri: uri, tableName: "records"})
	q.metrics.DeletesQueued.Add(context.Background(), 1)

	if len(q.buf) >= 1000 {
		q.flushLocked()
	}
}

// QueueBacklinkDelete queues a backlink source URI for deletion
func (q *DeleteQueue) QueueBacklinkDelete(uri string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.buf = append(q.buf, pendingDelete{uri: uri, tableName: "backlinks"})

	if len(q.buf) >= 1000 {
		q.flushLocked()
	}
}

// flushLoop periodically flushes the buffer to pending_deletes table
func (q *DeleteQueue) flushLoop() {
	defer q.wg.Done()
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			q.flush()
		case <-q.done:
			q.flush()
			return
		}
	}
}

func (q *DeleteQueue) flush() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.flushLocked()
}

func (q *DeleteQueue) flushLocked() {
	if len(q.buf) == 0 {
		return
	}

	deletes := q.buf
	q.buf = make([]pendingDelete, 0, 1000)

	q.wg.Add(1)
	go func() {
		defer q.wg.Done()
		q.insertPendingDeletes(deletes)
	}()
}

func (q *DeleteQueue) insertPendingDeletes(deletes []pendingDelete) {
	maxRetries := 5
	baseDelay := 100 * time.Millisecond

	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			time.Sleep(baseDelay * time.Duration(1<<uint(attempt-1)))
		}

		err := q.doInsertPendingDeletes(deletes)
		if err == nil {
			q.logger.Debug("queued pending deletes", "count", len(deletes))
			return
		}

		q.logger.Warn("insert pending deletes failed, retrying", "error", err, "attempt", attempt+1, "count", len(deletes))
	}

	q.logger.Error("insert pending deletes failed after retries", "count", len(deletes))
}

func (q *DeleteQueue) doInsertPendingDeletes(deletes []pendingDelete) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	batch, err := q.conn.PrepareBatch(ctx, `INSERT INTO attie.pending_deletes (uri, table_name)`)
	if err != nil {
		return fmt.Errorf("prepare batch: %w", err)
	}

	for _, d := range deletes {
		if err := batch.Append(d.uri, d.tableName); err != nil {
			q.logger.Error("failed to append pending delete", "error", err, "uri", d.uri)
		}
	}

	if err := batch.Send(); err != nil {
		return fmt.Errorf("send batch: %w", err)
	}

	return nil
}

// cleanupLoop runs the actual DELETE mutations periodically
func (q *DeleteQueue) cleanupLoop() {
	defer q.wg.Done()

	// Wait a bit before first cleanup to let system stabilize
	select {
	case <-time.After(1 * time.Minute):
	case <-q.done:
		return
	}

	ticker := time.NewTicker(q.cleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			q.runCleanup()
		case <-q.done:
			return
		}
	}
}

func (q *DeleteQueue) runCleanup() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	q.logger.Info("starting delete cleanup job")

	// Process records deletes
	recordsDeleted := q.processDeletes(ctx, "records", "uri")

	// Process backlinks deletes
	backlinksDeleted := q.processDeletes(ctx, "backlinks", "source_uri")

	q.logger.Info("delete cleanup job completed", "records_deleted", recordsDeleted, "backlinks_deleted", backlinksDeleted)
}

func (q *DeleteQueue) processDeletes(ctx context.Context, tableName, uriColumn string) int {
	// Capture cutoff time BEFORE processing to avoid TOCTOU race:
	// new deletes arriving after this point won't be cleared in step 2.
	var cutoff string
	err := q.conn.QueryRow(ctx, `SELECT toString(now())`).Scan(&cutoff)
	if err != nil {
		q.logger.Error("failed to get cutoff time", "table", tableName, "error", err)
		return 0
	}

	// Get count of pending deletes for this table
	var count uint64
	err = q.conn.QueryRow(ctx, `
		SELECT count() FROM attie.pending_deletes WHERE table_name = $1 AND created_at <= $2
	`, tableName, cutoff).Scan(&count)
	if err != nil {
		q.logger.Error("failed to count pending deletes", "table", tableName, "error", err)
		return 0
	}

	if count == 0 {
		return 0
	}

	q.logger.Info("processing pending deletes", "table", tableName, "count", count)

	// Run the DELETE mutation using only deletes before the cutoff
	deleteQuery := `
		ALTER TABLE attie.` + tableName + ` DELETE WHERE ` + uriColumn + ` IN (
			SELECT uri FROM attie.pending_deletes WHERE table_name = $1 AND created_at <= $2
		)
		SETTINGS allow_nondeterministic_mutations = 1
	`

	err = q.conn.Exec(ctx, deleteQuery, tableName, cutoff)
	if err != nil {
		q.logger.Error("failed to execute delete mutation", "table", tableName, "error", err)
		return 0
	}

	// Clear only the deletes we processed (before cutoff)
	err = q.conn.Exec(ctx, `
		ALTER TABLE attie.pending_deletes DELETE WHERE table_name = $1 AND created_at <= $2
	`, tableName, cutoff)
	if err != nil {
		q.logger.Warn("failed to clear pending deletes", "table", tableName, "error", err)
	}

	q.metrics.DeletesProcessed.Add(ctx, int64(count))
	return int(count)
}

// Close stops the delete queue and flushes remaining items
func (q *DeleteQueue) Close() error {
	close(q.done)

	// Wait with timeout to prevent hanging
	done := make(chan struct{})
	go func() {
		q.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		q.logger.Info("delete queue closed successfully")
	case <-time.After(5 * time.Second):
		q.logger.Warn("delete queue close timed out, some deletes may not be queued")
	}
	return nil
}
