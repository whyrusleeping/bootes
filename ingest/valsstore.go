package ingest

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/whyrusleeping/valsgo"
)

// ValsStore backs the ingester with a vals cluster instead of ClickHouse.
// One type implements every sink role (RecordSink, BacklinkSink, DeleteSink,
// CursorStore):
//
//   - records   → sorted zone "records",   key  {collection}/{did}/{rkey},
//     value = the Record as JSON. Sorted keys make per-collection (and
//     per-collection-per-repo) range scans cheap: `valsq scan
//     'app.bsky.feed.post/'`.
//   - backlinks → sorted zone "backlinks", key  {ref}\x00{source_uri}\x00{path},
//     so "who references X" is a prefix scan on X.
//   - deletes   → direct vals tombstones (no pending-delete machinery —
//     deletes are first-class and cheap). Backlink deletes by source URI are
//     NOT implemented (they would need a reverse index); stale backlinks
//     remain until a future cleanup pass. Documented limitation.
//   - cursor    → default-zone key "cursor/firehose".
//
// Writes are buffered and flushed through the batch group-commit path, with
// bounded retries (a refreshed ring + retry on cluster reconfiguration).
//
// The zones must exist before records flow: `valsd admin create-zone records
// 2 sorted` etc. — see run-bootes.sh in vals-examples.
type ValsStore struct {
	client *valsgo.Client
	logger *slog.Logger

	mu      sync.Mutex
	records []valsgo.KV
	links   []valsgo.KV
	closed  bool

	flushNow  chan struct{}
	done      chan struct{}
	flushedWg sync.WaitGroup
}

const (
	valsRecordZone   = "records"
	valsBacklinkZone = "backlinks"
	valsCursorKey    = "cursor/firehose"
	valsFlushSize    = 2000
	valsFlushAge     = 2 * time.Second
)

// NewValsStore dials the cluster and waits for the ingest zones to exist.
func NewValsStore(nodes map[uint32]string, logger *slog.Logger) (*ValsStore, error) {
	client, err := valsgo.Dial(valsgo.Config{Nodes: nodes, ClientID: 4000})
	if err != nil {
		return nil, fmt.Errorf("vals: %w", err)
	}
	// The zones are admin-created; wait briefly so a fresh cluster works.
	deadline := time.Now().Add(15 * time.Second)
	for {
		zones := client.ZoneNames()
		if contains(zones, valsRecordZone) && contains(zones, valsBacklinkZone) {
			break
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf(
				"vals: zones %q/%q missing — create them: valsd admin create-zone %s 2 sorted",
				valsRecordZone, valsBacklinkZone, valsRecordZone)
		}
		time.Sleep(200 * time.Millisecond)
		client.Refresh()
	}
	s := &ValsStore{
		client:   client,
		logger:   logger,
		flushNow: make(chan struct{}, 1),
		done:     make(chan struct{}),
	}
	s.flushedWg.Add(1)
	go s.flushLoop()
	return s, nil
}

func contains(xs []string, want string) bool {
	for _, x := range xs {
		if x == want {
			return true
		}
	}
	return false
}

// recordKey: {collection}/{did}/{rkey} — collection-first so a collection is
// one contiguous sorted range.
func recordKey(collection, did, rkey string) []byte {
	return []byte(collection + "/" + did + "/" + rkey)
}

// uriToRecordKey converts at://did/collection/rkey to the record key.
func uriToRecordKey(uri string) ([]byte, bool) {
	rest, ok := strings.CutPrefix(uri, "at://")
	if !ok {
		return nil, false
	}
	parts := strings.SplitN(rest, "/", 3)
	if len(parts) != 3 {
		return nil, false
	}
	return recordKey(parts[1], parts[0], parts[2]), true
}

// WriteRecord implements RecordSink.
func (s *ValsStore) WriteRecord(r Record) {
	blob, err := json.Marshal(r)
	if err != nil {
		s.logger.Error("vals: marshal record", "uri", r.URI, "error", err)
		return
	}
	s.push(&s.records, valsgo.KV{Key: recordKey(r.Collection, r.DID, r.Rkey), Value: blob})
}

// Write implements BacklinkSink.
func (s *ValsStore) Write(backlinks []BacklinkRecord) {
	for _, bl := range backlinks {
		blob, err := json.Marshal(bl)
		if err != nil {
			s.logger.Error("vals: marshal backlink", "ref", bl.Ref, "error", err)
			continue
		}
		key := []byte(bl.Ref + "\x00" + bl.SourceURI + "\x00" + bl.Path)
		s.push(&s.links, valsgo.KV{Key: key, Value: blob})
	}
}

func (s *ValsStore) push(buf *[]valsgo.KV, kv valsgo.KV) {
	s.mu.Lock()
	*buf = append(*buf, kv)
	full := len(*buf) >= valsFlushSize
	s.mu.Unlock()
	if full {
		select {
		case s.flushNow <- struct{}{}:
		default:
		}
	}
}

func (s *ValsStore) flushLoop() {
	defer s.flushedWg.Done()
	ticker := time.NewTicker(valsFlushAge)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
		case <-s.flushNow:
		case <-s.done:
			s.flush() // final drain
			return
		}
		s.flush()
	}
}

func (s *ValsStore) flush() {
	s.mu.Lock()
	records, links := s.records, s.links
	s.records, s.links = nil, nil
	s.mu.Unlock()
	s.flushBatch(valsRecordZone, records)
	s.flushBatch(valsBacklinkZone, links)
}

// flushBatch writes one buffered batch with bounded retries (a cluster
// reconfiguration mid-batch surfaces as an error once; the client refreshes
// and the retry lands).
func (s *ValsStore) flushBatch(zone string, kvs []valsgo.KV) {
	if len(kvs) == 0 {
		return
	}
	backoff := 250 * time.Millisecond
	for attempt := 0; ; attempt++ {
		err := s.client.PutBatch(zone, kvs)
		if err == nil {
			return
		}
		if attempt >= 6 {
			s.logger.Error("vals: dropping batch after retries", "zone", zone,
				"entries", len(kvs), "error", err)
			return
		}
		s.logger.Warn("vals: batch flush failed; retrying", "zone", zone,
			"entries", len(kvs), "attempt", attempt, "error", err)
		time.Sleep(backoff)
		backoff *= 2
		s.client.Refresh()
	}
}

// QueueRecordDelete implements DeleteSink: a direct vals tombstone.
func (s *ValsStore) QueueRecordDelete(uri string) {
	key, ok := uriToRecordKey(uri)
	if !ok {
		s.logger.Warn("vals: unparseable delete uri", "uri", uri)
		return
	}
	if _, err := s.client.Delete(valsRecordZone, key); err != nil {
		s.logger.Error("vals: delete failed", "uri", uri, "error", err)
	}
}

// QueueBacklinkDelete implements DeleteSink. Backlink keys are ref-first;
// deleting by *source* URI needs a reverse index this example doesn't build —
// stale backlinks stay until a cleanup pass (documented limitation).
func (s *ValsStore) QueueBacklinkDelete(uri string) {}

// LoadCursor implements CursorStore.
func (s *ValsStore) LoadCursor(ctx context.Context) (int64, error) {
	val, _, found, err := s.client.Get("", []byte(valsCursorKey))
	if err != nil {
		return 0, err
	}
	if !found {
		return 0, nil
	}
	cursor, err := strconv.ParseInt(string(val), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("vals: bad cursor value %q", val)
	}
	return cursor, nil
}

// SaveCursor implements CursorStore.
func (s *ValsStore) SaveCursor(ctx context.Context, cursor int64) error {
	_, err := s.client.Put("", []byte(valsCursorKey), []byte(strconv.FormatInt(cursor, 10)))
	return err
}

// Close drains the buffers. Safe to call once per sink role (the ingester
// closes each of its sinks; they all alias this store).
func (s *ValsStore) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.mu.Unlock()
	close(s.done)
	s.flushedWg.Wait()
	return nil
}
