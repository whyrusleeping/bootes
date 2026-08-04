package ingest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	comatproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/backfill"
	"github.com/bluesky-social/indigo/events"
	"github.com/bluesky-social/indigo/events/schedulers/parallel"
	"github.com/bluesky-social/indigo/repo"
	"github.com/bluesky-social/indigo/xrpc"
	"github.com/gorilla/websocket"
	"github.com/ipfs/go-cid"
	"gorm.io/gorm"
)

const (
	DefaultRelayURL          = "wss://bsky.network"
	CursorSaveInterval       = 5 * time.Second
	DefaultParallelBackfills = 50
)

// SelfLabelStore is the interface needed to persist self-labels extracted from posts.
type SelfLabelStore interface {
	ApplySelfLabels(uri, did string, vals []string) error
}

// Ingester coordinates firehose consumption and backfill
type Ingester struct {
	relayURL        string
	writer          RecordSink
	backlinkWriter  BacklinkSink
	deleteQueue     DeleteSink
	cursors         CursorStore
	disableBackfill bool
	store           *backfill.Gormstore
	backfiller      *backfill.Backfiller
	stopBackfiller  func(context.Context) error
	logger          *slog.Logger
	metrics         *Metrics
	httpClient      *http.Client
	xrpcClient      *xrpc.Client
	db              *gorm.DB
	clickhouse      driver.Conn
	labelStore      SelfLabelStore

	cursor     int64
	cursorMu   sync.RWMutex
	cursorDone chan struct{}

	admissionMu     sync.Mutex
	admissionClosed bool
	activeWork      sync.WaitGroup

	parallelBackfills int

	// Horizontal sharding: this instance backfills only repos whose DID hashes
	// into shard shardN of shardK (shardK<=1 means "all repos"). Lets several
	// bootes processes split the network with no overlap. disableFirehose skips
	// the live firehose entirely (worker shards; one primary runs it).
	shardN          int
	shardK          int
	disableFirehose bool
}

// Config holds ingester configuration
type Config struct {
	RelayURL          string
	BackfillDBPath    string
	ParallelBackfills int
	ClickHouseConn    driver.Conn
	Metrics           *Metrics
	LabelStore        SelfLabelStore
	// Cursors overrides where the firehose cursor persists (default: the
	// attie.cursors table on ClickHouseConn).
	Cursors CursorStore
	// DisableBackfill skips the repo pump + backfiller: firehose-only mode.
	DisableBackfill bool
	// DisableFirehose skips the live firehose subscription: backfill-only mode
	// (for worker shards; run one primary with the firehose enabled).
	DisableFirehose bool
	// ShardN/ShardK partition the repo pump by DID hash (ShardK<=1 = all repos).
	ShardN int
	ShardK int
}

// NewIngester creates a new ingester
func NewIngester(cfg Config, writer RecordSink, backlinkWriter BacklinkSink, deleteQueue DeleteSink, logger *slog.Logger) (*Ingester, error) {
	if cfg.RelayURL == "" {
		cfg.RelayURL = DefaultRelayURL
	}
	if cfg.ParallelBackfills == 0 {
		cfg.ParallelBackfills = DefaultParallelBackfills
	}

	store, db, err := NewBackfillStore(cfg.BackfillDBPath)
	if err != nil {
		return nil, fmt.Errorf("create backfill store: %w", err)
	}

	// XRPC client for repo sync
	xrpcClient := &xrpc.Client{
		Client: &http.Client{Timeout: 60 * time.Second},
		Host:   strings.Replace(cfg.RelayURL, "wss://", "https://", 1),
	}

	ing := &Ingester{
		relayURL:          cfg.RelayURL,
		writer:            writer,
		backlinkWriter:    backlinkWriter,
		deleteQueue:       deleteQueue,
		cursors:           cfg.Cursors,
		disableBackfill:   cfg.DisableBackfill,
		store:             store,
		logger:            logger,
		metrics:           cfg.Metrics,
		httpClient:        &http.Client{Timeout: 30 * time.Second},
		xrpcClient:        xrpcClient,
		db:                db,
		clickhouse:        cfg.ClickHouseConn,
		labelStore:        cfg.LabelStore,
		cursorDone:        make(chan struct{}),
		parallelBackfills: cfg.ParallelBackfills,
		shardN:            cfg.ShardN,
		shardK:            cfg.ShardK,
		disableFirehose:   cfg.DisableFirehose,
	}

	// Create backfiller with indigo's backfill package
	opts := backfill.DefaultBackfillOptions()
	opts.ParallelBackfills = cfg.ParallelBackfills
	opts.ParallelRecordCreates = 40
	opts.SyncRequestsPerSecond = 500
	opts.PDSRequestsPerSecond = 10
	opts.NSIDFilter = "" // All record types
	opts.RelayHost = strings.Replace(cfg.RelayURL, "wss://", "https://", 1)
	/*
		opts.Client = &http.Client{
			Transport: &http.Transport{
				MaxIdleConns:          cfg.ParallelBackfills * 2,
				MaxIdleConnsPerHost:   max(cfg.ParallelBackfills/50, 10),
				MaxConnsPerHost:       max(cfg.ParallelBackfills/20, 20),
				IdleConnTimeout:       90 * time.Second,
				TLSHandshakeTimeout:   10 * time.Second,
				ResponseHeaderTimeout: 30 * time.Second,
			},
			Timeout: 600 * time.Second,
		}
	*/

	ing.backfiller = backfill.NewBackfiller(
		"attie",
		store,
		ing.handleBackfillCreate,
		ing.handleBackfillUpdate,
		ing.handleBackfillDelete,
		opts,
	)
	ing.stopBackfiller = ing.backfiller.Stop

	return ing, nil
}

// Run starts the ingester
func (i *Ingester) Run(ctx context.Context) error {
	// Load pending jobs
	if err := i.store.LoadJobs(ctx); err != nil {
		i.logger.Warn("failed to load jobs", "error", err)
	}

	// Load cursor from ClickHouse
	if err := i.loadCursor(ctx); err != nil {
		i.logger.Warn("failed to load cursor from clickhouse, starting from latest", "error", err)
	}

	// Context-closing stores (currently vals) buffer writes. Persisting their
	// cursor periodically could put it ahead of undrained data, so they only save
	// the cursor after the final successful drain in CloseContext.
	if _, buffered := i.cursors.(ContextCloser); !buffered {
		go i.cursorSaver(ctx)
	}

	// Start backfill processor + initial repo pump (unless firehose-only)
	if !i.disableBackfill {
		go i.backfiller.Start()
		go i.pumpRepos(ctx)
	}

	// Run firehose consumer (unless this is a backfill-only worker shard)
	if i.disableFirehose {
		i.logger.Info("firehose disabled (backfill-only); running until stopped")
		<-ctx.Done()
		return ctx.Err()
	}
	return i.runFirehose(ctx)
}

// inShard reports whether a repo DID belongs to this instance's shard. With
// shardK<=1 (the default) every DID is in-shard. Uses FNV-1a so the partition
// is stable across instances and runs.
func (i *Ingester) inShard(did string) bool {
	if i.shardK <= 1 {
		return true
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(did))
	return int(h.Sum32()%uint32(i.shardK)) == i.shardN
}

// loadCursor loads the firehose cursor from ClickHouse
func (i *Ingester) loadCursor(ctx context.Context) error {
	var cursor int64
	if i.cursors != nil {
		c, err := i.cursors.LoadCursor(ctx)
		if err != nil {
			return fmt.Errorf("load cursor: %w", err)
		}
		cursor = c
	} else {
		if i.clickhouse == nil {
			return fmt.Errorf("no clickhouse connection")
		}
		err := i.clickhouse.QueryRow(ctx, `
		SELECT cursor FROM attie.cursors FINAL WHERE name = 'firehose' LIMIT 1
	`).Scan(&cursor)
		if err != nil {
			return fmt.Errorf("query cursor: %w", err)
		}
	}

	if cursor > 0 {
		i.cursorMu.Lock()
		i.cursor = cursor
		i.cursorMu.Unlock()
		i.logger.Info("loaded firehose cursor", "seq", cursor)
	}

	return nil
}

// pumpRepos fetches all existing repos and queues them for backfill
// It tracks progress in SQLite and resumes from where it left off if interrupted
func (i *Ingester) pumpRepos(ctx context.Context) {
	const pumpName = "repo_pump"

	// Check if pump already completed
	var progress PumpProgress
	if err := i.db.Where("name = ?", pumpName).First(&progress).Error; err == nil {
		if progress.Completed {
			i.logger.Info("initial repo pump already completed, skipping", "total", progress.TotalPumped)
			return
		}
		// Resume from last position
		i.logger.Info("resuming repo pump from checkpoint", "cursor", progress.Cursor, "total_so_far", progress.TotalPumped)
	} else {
		// Create initial progress record
		progress = PumpProgress{Name: pumpName}
		if err := i.db.Create(&progress).Error; err != nil {
			i.logger.Error("failed to create pump progress record", "error", err)
			return
		}
	}

	cursor := progress.Cursor
	total := progress.TotalPumped
	lastSave := time.Now()

	i.logger.Info("starting repo pump", "resuming_from", cursor, "total_so_far", total,
		"shard", i.shardN, "shard_count", i.shardK, "firehose_enabled", !i.disableFirehose)

	for {
		select {
		case <-ctx.Done():
			// Save progress before exiting
			if err := i.db.Model(&progress).Updates(map[string]any{
				"cursor":       cursor,
				"total_pumped": total,
				"updated_at":   time.Now(),
			}).Error; err != nil {
				i.logger.Error("failed to save pump progress on shutdown", "error", err)
			}
			i.logger.Info("repo pump interrupted, progress saved", "cursor", cursor, "total", total)
			return
		default:
		}

		resp, err := comatproto.SyncListRepos(ctx, i.xrpcClient, cursor, 1000)
		if err != nil {
			i.logger.Error("list repos", "error", err)
			time.Sleep(5 * time.Second)
			continue
		}

		for _, r := range resp.Repos {
			if !i.inShard(r.Did) {
				continue
			}
			if err := i.store.EnqueueJob(ctx, r.Did); err != nil {
				// Ignore duplicate errors
				if !strings.Contains(err.Error(), "already exists") {
					i.logger.Error("enqueue backfill job", "error", err, "did", r.Did)
				}
			}
			total++
		}

		if resp.Cursor == nil || *resp.Cursor == "" {
			break
		}
		cursor = *resp.Cursor

		// Save progress periodically (every 30 seconds)
		if time.Since(lastSave) > 30*time.Second {
			if err := i.db.Model(&progress).Updates(map[string]any{
				"cursor":       cursor,
				"total_pumped": total,
				"updated_at":   time.Now(),
			}).Error; err != nil {
				i.logger.Error("failed to save pump progress checkpoint", "error", err)
			}
			lastSave = time.Now()
			i.logger.Info("pumped repos (checkpoint saved)", "total", total, "cursor", cursor)
		} else {
			i.logger.Info("pumped repos", "total", total, "cursor", cursor)
		}
	}

	// Mark as completed
	if err := i.db.Model(&progress).Updates(map[string]any{
		"cursor":       cursor,
		"total_pumped": total,
		"completed":    true,
		"updated_at":   time.Now(),
	}).Error; err != nil {
		i.logger.Error("failed to mark pump as completed", "error", err)
	}

	i.logger.Info("initial repo pump complete", "total", total)
}

// runFirehose connects to the firehose and processes events
func (i *Ingester) runFirehose(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if err := i.consumeFirehose(ctx); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			i.logger.Error("firehose error, reconnecting", "error", err)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(2 * time.Second):
			}
		}
	}
}

// consumeFirehose connects and processes events until disconnected
func (i *Ingester) consumeFirehose(ctx context.Context) error {
	url := fmt.Sprintf("%s/xrpc/com.atproto.sync.subscribeRepos", i.relayURL)

	i.cursorMu.RLock()
	cursor := i.cursor
	i.cursorMu.RUnlock()

	if cursor > 0 {
		url = fmt.Sprintf("%s?cursor=%d", url, cursor)
	}

	i.logger.Info("connecting to firehose", "url", url)

	conn, _, err := websocket.DefaultDialer.DialContext(ctx, url, nil) //nolint:bodyclose // websocket conn, not a regular HTTP response
	if err != nil {
		return fmt.Errorf("dial firehose: %w", err)
	}
	defer conn.Close()

	// Close the websocket when context is cancelled so reads unblock
	go func() {
		<-ctx.Done()
		conn.Close()
	}()

	i.logger.Info("connected to firehose")

	// Use parallel scheduler for event processing
	scheduler := parallel.NewScheduler(
		200,  // max concurrent
		1000, // buffer size
		i.relayURL,
		i.handleEvent,
	)

	return events.HandleRepoStream(ctx, conn, scheduler, i.logger)
}

func (i *Ingester) beginWork() bool {
	i.admissionMu.Lock()
	defer i.admissionMu.Unlock()
	if i.admissionClosed {
		return false
	}
	i.activeWork.Add(1)
	return true
}

func (i *Ingester) endWork() { i.activeWork.Done() }

// StopAdmission prevents callbacks that were queued but not started from
// becoming producers. Already-active callbacks remain counted until return.
func (i *Ingester) StopAdmission() {
	i.admissionMu.Lock()
	i.admissionClosed = true
	i.admissionMu.Unlock()
}

func (i *Ingester) waitForActiveWork(ctx context.Context) error {
	done := make(chan struct{})
	go func() { i.activeWork.Wait(); close(done) }()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// BeginShutdown immediately stops callback admission and notifies buffered
// sinks so an already-sleeping retry switches to the shared deadline.
func (i *Ingester) BeginShutdown(ctx context.Context) {
	i.StopAdmission()
	seen := make(map[any]bool)
	for _, sink := range []any{i.writer, i.backlinkWriter, i.deleteQueue} {
		if sink == nil || seen[sink] {
			continue
		}
		seen[sink] = true
		if notifier, ok := sink.(ShutdownNotifier); ok {
			notifier.BeginShutdown(ctx)
		}
	}
}

func (i *Ingester) reportUnclosedSinks(reason error) {
	seen := make(map[any]bool)
	for _, sink := range []any{i.writer, i.backlinkWriter, i.deleteQueue} {
		if sink == nil || seen[sink] {
			continue
		}
		seen[sink] = true
		if reporter, ok := sink.(ShutdownReporter); ok {
			reporter.ReportShutdownIncomplete(reason)
		}
	}
}

// handleEvent processes a single firehose event.
func (i *Ingester) handleEvent(ctx context.Context, evt *events.XRPCStreamEvent) error {
	if !i.beginWork() {
		return context.Canceled
	}
	defer i.endWork()
	i.metrics.FirehoseEvents.Add(ctx, 1)
	switch {
	case evt.RepoCommit != nil:
		return i.handleCommit(ctx, evt.RepoCommit)
	case evt.RepoIdentity != nil:
		return i.handleIdentity(ctx, evt.RepoIdentity)
	}
	return nil
}

// handleCommit processes a repo commit event
func (i *Ingester) handleCommit(ctx context.Context, commit *comatproto.SyncSubscribeRepos_Commit) error {
	// Update cursor
	i.cursorMu.Lock()
	i.cursor = commit.Seq
	i.cursorMu.Unlock()
	i.metrics.FirehoseSeq.Record(ctx, commit.Seq)

	// Check if repo has a backfill job
	job, err := i.store.GetJob(ctx, commit.Repo)
	jobExists := err == nil

	// If no job exists, create one and process event directly
	if !jobExists {
		if err := i.store.EnqueueJob(ctx, commit.Repo); err != nil {
			if !strings.Contains(err.Error(), "already exists") {
				i.logger.Error("enqueue job for new repo", "error", err, "repo", commit.Repo)
			}
		}
	}

	// Check job state - buffer if backfilling
	backfilling := jobExists && (job.State() == backfill.StateInProgress || job.State() == backfill.StateEnqueued)

	// Parse repo operations
	rr, err := repo.ReadRepoFromCar(ctx, bytes.NewReader([]byte(commit.Blocks)))
	if err != nil {
		// Not fatal, might be partial commit
		i.logger.Debug("read repo from car", "error", err, "repo", commit.Repo)
	}

	for _, op := range commit.Ops {
		uri := fmt.Sprintf("at://%s/%s", commit.Repo, op.Path)
		parts := strings.SplitN(op.Path, "/", 2)
		if len(parts) != 2 {
			continue
		}
		collection, rkey := parts[0], parts[1]

		switch op.Action {
		case "create", "update":
			if backfilling {
				// Buffer the operation for processing after backfill completes
				// The backfill package handles this internally
				continue
			}

			if rr == nil || op.Cid == nil {
				continue
			}

			// Get raw CBOR bytes from repo
			_, recBytes, err := rr.GetRecordBytes(ctx, op.Path)
			if err != nil {
				i.logger.Debug("get record bytes", "error", err, "path", op.Path)
				continue
			}
			if recBytes == nil || len(*recBytes) == 0 {
				continue
			}

			i.processRawRecord(commit.Repo, collection, rkey, uri, *recBytes, uint64(commit.Seq))

		case "delete":
			uri := fmt.Sprintf("at://%s/%s/%s", commit.Repo, collection, rkey)
			// Queue deletions for periodic cleanup (avoids mutation overhead)
			if i.deleteQueue != nil {
				i.deleteQueue.QueueRecordDelete(uri)
				i.deleteQueue.QueueBacklinkDelete(uri)
			}
		}
	}

	return nil
}

// handleIdentity processes identity updates
func (i *Ingester) handleIdentity(ctx context.Context, identity *comatproto.SyncSubscribeRepos_Identity) error {
	// We don't store handles - they can change. Use resolve_handle tool for lookups.
	// Identity events are acknowledged but we only track DIDs.
	return nil
}

// handleBackfillCreate handles record creates during backfill
func (i *Ingester) handleBackfillCreate(ctx context.Context, repo string, rev string, path string, rec *[]byte, cidVal *cid.Cid) error {
	if !i.beginWork() {
		return context.Canceled
	}
	defer i.endWork()
	return i.handleBackfillCreateActive(ctx, repo, rev, path, rec, cidVal)
}

func (i *Ingester) handleBackfillCreateActive(ctx context.Context, repo string, rev string, path string, rec *[]byte, cidVal *cid.Cid) error {
	parts := strings.SplitN(path, "/", 2)
	if len(parts) != 2 {
		return nil
	}
	collection, rkey := parts[0], parts[1]
	uri := fmt.Sprintf("at://%s/%s", repo, path)

	if rec == nil || len(*rec) == 0 {
		return nil
	}

	i.processRawRecord(repo, collection, rkey, uri, *rec, 0)
	i.metrics.BackfillRecords.Add(ctx, 1)
	return nil
}

// handleBackfillUpdate handles record updates during backfill
func (i *Ingester) handleBackfillUpdate(ctx context.Context, repo string, rev string, path string, rec *[]byte, cidVal *cid.Cid) error {
	if !i.beginWork() {
		return context.Canceled
	}
	defer i.endWork()
	return i.handleBackfillCreateActive(ctx, repo, rev, path, rec, cidVal)
}

// handleBackfillDelete handles record deletes during backfill
func (i *Ingester) handleBackfillDelete(ctx context.Context, repo string, rev string, path string) error {
	if !i.beginWork() {
		return context.Canceled
	}
	defer i.endWork()
	uri := fmt.Sprintf("at://%s/%s", repo, path)
	// Queue deletions for periodic cleanup (avoids mutation overhead)
	if i.deleteQueue != nil {
		i.deleteQueue.QueueRecordDelete(uri)
		i.deleteQueue.QueueBacklinkDelete(uri)
	}
	return nil
}

// processRawRecord processes raw CBOR bytes - unified path for both backfill and firehose
func (i *Ingester) processRawRecord(did, collection, rkey, uri string, rawCBOR []byte, seq uint64) {
	// Fast path: single-pass CBOR->JSON + metadata extraction
	result := FastCBORToJSON(rawCBOR, rkey)
	i.writer.WriteRecord(Record{
		URI:        uri,
		DID:        did,
		Collection: collection,
		Rkey:       rkey,
		Record:     result.JSON,
		CreatedAt:  result.CreatedAt,
		IndexedAt:  time.Now(),
		Seq:        seq,
	})

	// Write backlinks
	i.writeBacklinks(result.Backlinks, collection, uri, did, result.CreatedAt)

	// Extract self-labels from post records
	if i.labelStore != nil && collection == "app.bsky.feed.post" {
		if vals := extractSelfLabels(result.JSON); len(vals) > 0 {
			if err := i.labelStore.ApplySelfLabels(uri, did, vals); err != nil {
				i.logger.Error("failed to apply self-labels", "error", err, "uri", uri)
			}
		}
	}
}

// extractSelfLabels parses post JSON and returns any self-label values.
func extractSelfLabels(postJSON string) []string {
	var post struct {
		Labels *struct {
			Values []struct {
				Val string `json:"val"`
			} `json:"values"`
		} `json:"labels"`
	}
	if err := json.Unmarshal([]byte(postJSON), &post); err != nil || post.Labels == nil {
		return nil
	}
	vals := make([]string, 0, len(post.Labels.Values))
	for _, v := range post.Labels.Values {
		if v.Val != "" {
			vals = append(vals, v.Val)
		}
	}
	return vals
}

// cursorSaver periodically saves the cursor to ClickHouse
func (i *Ingester) cursorSaver(ctx context.Context) {
	ticker := time.NewTicker(CursorSaveInterval)
	defer ticker.Stop()

	var lastSaved int64

	for {
		select {
		case <-ctx.Done():
			return
		case <-i.cursorDone:
			return
		case <-ticker.C:
			i.cursorMu.RLock()
			cursor := i.cursor
			i.cursorMu.RUnlock()

			if cursor > 0 && cursor != lastSaved {
				if err := i.persistCursor(cursor); err != nil {
					i.logger.Warn("failed to save cursor", "error", err, "seq", cursor)
				} else {
					lastSaved = cursor
					i.logger.Debug("cursor saved", "seq", cursor)
				}
			}
		}
	}
}

// saveCursor saves the current cursor value (used during shutdown).
func (i *Ingester) saveCursor(ctx context.Context) error {
	i.cursorMu.RLock()
	cursor := i.cursor
	i.cursorMu.RUnlock()

	if cursor == 0 {
		return nil
	}
	if err := i.persistCursorContext(ctx, cursor); err != nil {
		i.logger.Error("failed to save cursor on shutdown", "error", err, "seq", cursor)
		return err
	}
	i.logger.Info("cursor saved on shutdown", "seq", cursor)
	return nil
}

// persistCursor writes the cursor to ClickHouse.
func (i *Ingester) persistCursor(cursor int64) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return i.persistCursorContext(ctx, cursor)
}

func (i *Ingester) persistCursorContext(ctx context.Context, cursor int64) error {
	if i.cursors != nil {
		return i.cursors.SaveCursor(ctx, cursor)
	}
	if i.clickhouse == nil {
		return fmt.Errorf("no clickhouse connection")
	}
	return i.clickhouse.Exec(ctx, `
		INSERT INTO attie.cursors (name, cursor, updated_at) VALUES ('firehose', $1, now64(3))
	`, cursor)
}

// writeBacklinks converts and writes backlinks to the BacklinkWriter
func (i *Ingester) writeBacklinks(backlinks []Backlink, collection, sourceURI, sourceDID string, createdAt time.Time) {
	if i.backlinkWriter == nil || len(backlinks) == 0 {
		return
	}

	records := make([]BacklinkRecord, 0, len(backlinks))
	for _, bl := range backlinks {
		records = append(records, BacklinkRecord{
			Ref:        bl.Ref,
			RefType:    bl.RefType,
			Collection: collection,
			Path:       bl.Path,
			SourceURI:  sourceURI,
			SourceDID:  sourceDID,
			CreatedAt:  createdAt,
		})
	}
	i.backlinkWriter.Write(records)
}

// CloseContext shuts down under one caller-owned deadline. The cursor is only
// advanced after every distinct sink reports a successful drain.
func (i *Ingester) CloseContext(ctx context.Context) error {
	i.logger.Info("shutting down ingester...")
	select {
	case <-i.cursorDone:
	default:
		close(i.cursorDone)
	}

	i.BeginShutdown(ctx)
	var errs []error
	if !i.disableBackfill && i.stopBackfiller != nil {
		i.logger.Info("stopping backfiller...")
		if err := i.stopBackfiller(ctx); err != nil {
			err = fmt.Errorf("stop backfiller: %w", err)
			i.reportUnclosedSinks(err)
			i.logger.Error("final cursor persistence withheld: producers not quiesced", "error", err)
			i.logger.Error("ingester shutdown incomplete", "error", err, "deadline_expired", ctx.Err() != nil)
			return err
		}
	}
	if err := i.waitForActiveWork(ctx); err != nil {
		err = fmt.Errorf("wait for active producers: %w", err)
		i.reportUnclosedSinks(err)
		i.logger.Error("final cursor persistence withheld: producers not quiesced", "error", err)
		i.logger.Error("ingester shutdown incomplete", "error", err, "deadline_expired", true)
		return err
	}

	// Vals aliases all roles to one object; close each unique interface once.
	seen := make(map[any]bool)
	for _, sink := range []any{i.writer, i.backlinkWriter, i.deleteQueue} {
		if sink == nil || seen[sink] {
			continue
		}
		seen[sink] = true
		var err error
		if closer, ok := sink.(ContextCloser); ok {
			err = closer.CloseContext(ctx)
		} else {
			done := make(chan error, 1)
			go func(v any) {
				switch s := v.(type) {
				case RecordSink:
					done <- s.Close()
				case BacklinkSink:
					done <- s.Close()
				case DeleteSink:
					done <- s.Close()
				}
			}(sink)
			select {
			case err = <-done:
			case <-ctx.Done():
				err = ctx.Err()
			}
		}
		if err != nil {
			errs = append(errs, fmt.Errorf("drain sink: %w", err))
		}
	}

	if len(errs) == 0 {
		if err := i.saveCursor(ctx); err != nil {
			errs = append(errs, fmt.Errorf("persist cursor: %w", err))
		}
	} else {
		i.logger.Error("final cursor persistence withheld: data drain incomplete", "errors", errors.Join(errs...))
	}
	if err := errors.Join(errs...); err != nil {
		i.logger.Error("ingester shutdown incomplete", "error", err, "deadline_expired", ctx.Err() != nil)
		return err
	}
	i.logger.Info("ingester shutdown complete")
	return nil
}

func (i *Ingester) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return i.CloseContext(ctx)
}
