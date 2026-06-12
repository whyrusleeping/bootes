package ingest

import "context"

// The sink interfaces the Ingester writes through. The ClickHouse types
// (BatchWriter / BacklinkWriter / DeleteQueue) satisfy them as-is; an
// alternative backend (e.g. vals, see valsstore.go) plugs in here.

// RecordSink receives every ingested record.
type RecordSink interface {
	WriteRecord(r Record)
	Close() error
}

// BacklinkSink receives the backlinks extracted from each record.
type BacklinkSink interface {
	Write(backlinks []BacklinkRecord)
	Close() error
}

// DeleteSink receives record/backlink deletions (by AT-URI).
type DeleteSink interface {
	QueueRecordDelete(uri string)
	QueueBacklinkDelete(uri string)
	Close() error
}

// CursorStore persists the firehose cursor across restarts.
type CursorStore interface {
	LoadCursor(ctx context.Context) (int64, error)
	SaveCursor(ctx context.Context, cursor int64) error
}
