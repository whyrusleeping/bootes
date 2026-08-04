package ingest

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/whyrusleeping/valsgo"
)

type fakeValsClient struct {
	mu           sync.Mutex
	busy         bool
	batches      int
	puts         int
	batchStarted chan struct{}
	putBlock     <-chan struct{}
}

func (f *fakeValsClient) PutBatch(_ string, kvs []valsgo.KV) error {
	f.mu.Lock()
	f.batches += len(kvs)
	busy, started := f.busy, f.batchStarted
	f.mu.Unlock()
	if started != nil {
		select {
		case started <- struct{}{}:
		default:
		}
	}
	if busy {
		return valsgo.ErrBusy
	}
	return nil
}
func (f *fakeValsClient) Put(_ string, _, _ []byte) (valsgo.Version, error) {
	f.mu.Lock()
	f.puts++
	block := f.putBlock
	f.mu.Unlock()
	if block != nil {
		<-block
	}
	return valsgo.Version{}, nil
}
func (f *fakeValsClient) Delete(string, []byte) (valsgo.Version, error) { return valsgo.Version{}, nil }
func (f *fakeValsClient) Get(string, []byte) ([]byte, valsgo.Version, bool, error) {
	return nil, valsgo.Version{}, false, nil
}
func (f *fakeValsClient) Refresh() error      { return nil }
func (f *fakeValsClient) ZoneNames() []string { return []string{valsRecordZone, valsBacklinkZone} }

func testValsStore(client valsClient) *ValsStore {
	s := &ValsStore{client: client, logger: slog.New(slog.NewTextHandler(io.Discard, nil)), flushNow: make(chan struct{}, 1), done: make(chan struct{}), closeFinished: make(chan struct{}), shutdownWake: make(chan struct{})}
	s.drain = sync.NewCond(&s.mu)
	s.flushedWg.Add(1)
	go s.flushLoop()
	return s
}

func TestValsShutdownBusyHonorsDeadlineAndRejectsLateWrites(t *testing.T) {
	started := make(chan struct{}, 1)
	client := &fakeValsClient{busy: true, batchStarted: started}
	s := testValsStore(client)
	s.WriteRecord(Record{Collection: "c", DID: "d", Rkey: "r"})
	s.flushNow <- struct{}{}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("Busy retry did not start")
	}
	// The first Busy call is now sleeping in its pre-shutdown backoff.
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	closeStarted := time.Now()
	if err := s.CloseContext(ctx); err == nil {
		t.Fatal("Busy shutdown unexpectedly succeeded")
	}
	if time.Since(closeStarted) > 250*time.Millisecond {
		t.Fatal("shutdown exceeded bound")
	}
	s.WriteRecord(Record{Collection: "c", DID: "d", Rkey: "late"})
	stats := s.Stats()
	if stats.DroppedRecords+stats.InflightRecords != 1 || stats.RejectedRecords != 1 {
		t.Fatalf("stats = %+v", stats)
	}
}

func TestValsCursorPutHonorsDeadline(t *testing.T) {
	block := make(chan struct{})
	client := &fakeValsClient{putBlock: block}
	s := testValsStore(client)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	started := time.Now()
	if err := s.SaveCursor(ctx, 42); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("SaveCursor error = %v", err)
	}
	if time.Since(started) > 250*time.Millisecond {
		t.Fatal("cursor Put exceeded deadline")
	}
	close(block)
	if err := s.CloseContext(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestValsShutdownHealthyDrainAccounting(t *testing.T) {
	client := &fakeValsClient{}
	s := testValsStore(client)
	s.WriteRecord(Record{Collection: "c", DID: "d", Rkey: "r"})
	s.Write([]BacklinkRecord{{Ref: "at://ref", SourceURI: "at://src", Path: "x"}})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := s.CloseContext(ctx); err != nil {
		t.Fatal(err)
	}
	stats := s.Stats()
	if stats.PersistedRecords != 1 || stats.PersistedBacklinks != 1 || stats.DroppedRecords != 0 {
		t.Fatalf("stats = %+v", stats)
	}
}

type orderedSink struct {
	mu     sync.Mutex
	closed bool
}

func (s *orderedSink) WriteRecord(Record) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		panic("write after close")
	}
}
func (s *orderedSink) Close() error { return errors.New("legacy close should not run") }
func (s *orderedSink) CloseContext(context.Context) error {
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	return nil
}

type orderedCursor struct {
	sink  *orderedSink
	saved bool
}

func (*orderedCursor) LoadCursor(context.Context) (int64, error) { return 0, nil }
func (c *orderedCursor) SaveCursor(context.Context, int64) error {
	c.sink.mu.Lock()
	defer c.sink.mu.Unlock()
	if !c.sink.closed {
		return errors.New("cursor saved before drain")
	}
	c.saved = true
	return nil
}

func TestIngesterShutdownWaitsForActiveProducerBeforeDrainAndCursor(t *testing.T) {
	sink := &orderedSink{}
	cursor := &orderedCursor{sink: sink}
	i := &Ingester{writer: sink, cursors: cursor, disableBackfill: true, logger: slog.New(slog.NewTextHandler(io.Discard, nil)), cursorDone: make(chan struct{}), cursor: 42}
	if !i.beginWork() {
		t.Fatal("producer admission rejected before shutdown")
	}
	producerRelease := make(chan struct{})
	producerDone := make(chan struct{})
	go func() {
		defer close(producerDone)
		defer i.endWork()
		<-producerRelease
		sink.WriteRecord(Record{})
	}()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	closeDone := make(chan error, 1)
	go func() { closeDone <- i.CloseContext(ctx) }()
	time.Sleep(20 * time.Millisecond)
	sink.mu.Lock()
	closed := sink.closed
	sink.mu.Unlock()
	if closed || cursor.saved {
		t.Fatal("sink/cursor advanced before active producer quiesced")
	}
	close(producerRelease)
	<-producerDone
	if err := <-closeDone; err != nil {
		t.Fatal(err)
	}
	if !cursor.saved {
		t.Fatal("cursor was not persisted after producer and drain")
	}
}

func TestIngesterShutdownProducerQuiescenceDeadlineWithholdsCursor(t *testing.T) {
	sink := &orderedSink{}
	cursor := &orderedCursor{sink: sink}
	i := &Ingester{writer: sink, cursors: cursor, disableBackfill: true, logger: slog.New(slog.NewTextHandler(io.Discard, nil)), cursorDone: make(chan struct{}), cursor: 42}
	if !i.beginWork() {
		t.Fatal("producer admission rejected before shutdown")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := i.CloseContext(ctx); err == nil {
		t.Fatal("producer quiescence unexpectedly succeeded")
	}
	sink.mu.Lock()
	closed := sink.closed
	sink.mu.Unlock()
	if closed || cursor.saved {
		t.Fatal("sink closed or cursor persisted while producer active")
	}
	i.endWork()
}

func TestIngesterShutdownBackfillBoundAndCursorOrdering(t *testing.T) {
	sink := &orderedSink{}
	cursor := &orderedCursor{sink: sink}
	i := &Ingester{writer: sink, cursors: cursor, logger: slog.New(slog.NewTextHandler(io.Discard, nil)), cursorDone: make(chan struct{}), cursor: 42}
	i.stopBackfiller = func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := i.CloseContext(ctx); err == nil {
		t.Fatal("in-flight backfill shutdown unexpectedly succeeded")
	}
	if cursor.saved {
		t.Fatal("cursor persisted after backfill stop failure")
	}

	i2 := &Ingester{writer: sink, cursors: cursor, disableBackfill: true, logger: slog.New(slog.NewTextHandler(io.Discard, nil)), cursorDone: make(chan struct{}), cursor: 42}
	ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second)
	defer cancel2()
	if err := i2.CloseContext(ctx2); err != nil {
		t.Fatal(err)
	}
	if !cursor.saved {
		t.Fatal("cursor was not persisted after healthy drain")
	}
}
