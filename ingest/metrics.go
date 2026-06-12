package ingest

import (
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

// Metrics holds all ingest-related OTel metrics.
type Metrics struct {
	// Records
	RecordsWritten metric.Int64Counter
	RecordsFailed  metric.Int64Counter
	BatchSize      metric.Int64Histogram
	BatchFlushes   metric.Int64Counter

	// Backlinks
	BacklinksWritten metric.Int64Counter

	// Deletes
	DeletesQueued    metric.Int64Counter
	DeletesProcessed metric.Int64Counter

	// Firehose
	FirehoseEvents metric.Int64Counter
	FirehoseSeq    metric.Int64Gauge

	// Backfill
	BackfillRecords metric.Int64Counter
}

// NewMetrics creates and registers all ingest metrics.
func NewMetrics() *Metrics {
	meter := otel.Meter("ingest")

	m := &Metrics{}
	m.RecordsWritten, _ = meter.Int64Counter("ingest.records.written",
		metric.WithDescription("Total records written to ClickHouse"))
	m.RecordsFailed, _ = meter.Int64Counter("ingest.records.failed",
		metric.WithDescription("Total records failed after retries"))
	m.BatchSize, _ = meter.Int64Histogram("ingest.batch.size",
		metric.WithDescription("Records per batch flush"))
	m.BatchFlushes, _ = meter.Int64Counter("ingest.batch.flushes",
		metric.WithDescription("Total batch flush operations"))
	m.BacklinksWritten, _ = meter.Int64Counter("ingest.backlinks.written",
		metric.WithDescription("Total backlinks written to ClickHouse"))
	m.DeletesQueued, _ = meter.Int64Counter("ingest.deletes.queued",
		metric.WithDescription("Total delete operations queued"))
	m.DeletesProcessed, _ = meter.Int64Counter("ingest.deletes.processed",
		metric.WithDescription("Total delete mutations processed"))
	m.FirehoseEvents, _ = meter.Int64Counter("ingest.firehose.events",
		metric.WithDescription("Total firehose events received"))
	m.FirehoseSeq, _ = meter.Int64Gauge("ingest.firehose.seq",
		metric.WithDescription("Current firehose sequence number"))
	m.BackfillRecords, _ = meter.Int64Counter("ingest.backfill.records",
		metric.WithDescription("Total records processed via backfill"))

	return m
}
