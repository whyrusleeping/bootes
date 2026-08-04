# bootes

AT Protocol firehose ingester that writes to ClickHouse. Connects to a relay (default: `wss://bsky.network`), consumes the full event stream, and stores every record in ClickHouse with automatic backfilling of historical data.

Features:
- Batched writes with per-partition buffering
- Backlink extraction (DIDs and AT-URIs referenced in records)
- Deferred delete processing
- Automatic ClickHouse schema bootstrap (including replicated tables)
- Prometheus metrics and pprof endpoints
- Backfill state tracking via SQLite or Postgres
- Graceful shutdown with cursor persistence

## Requirements

- Go 1.26+
- ClickHouse (native protocol, port 9000)
- Optionally, Postgres or SQLite for backfill state

## Build

```
go build -o bootes .
```

## Usage

```
./bootes --clickhouse localhost:9000
```

All flags can also be set via environment variables:

| Flag | Env | Default | Description |
|------|-----|---------|-------------|
| `--clickhouse` | `ATTIE_CLICKHOUSE` | `localhost:9000` | ClickHouse address (native protocol) |
| `--clickhouse-user` | `ATTIE_CLICKHOUSE_USER` | `attie` | ClickHouse username |
| `--clickhouse-password` | `ATTIE_CLICKHOUSE_PASSWORD` | `attie` | ClickHouse password |
| `--clickhouse-bootstrap-nodes` | `ATTIE_CLICKHOUSE_BOOTSTRAP_NODES` | | Additional nodes for bootstrap DDL |
| `--clickhouse-readonly-user` | `ATTIE_CLICKHOUSE_READONLY_USER` | `attie-readonly` | Read-only user created during bootstrap |
| `--clickhouse-readonly-password` | `ATTIE_CLICKHOUSE_READONLY_PASSWORD` | | Read-only user password |
| `--relay` | `ATTIE_RELAY` | `wss://bsky.network` | Relay WebSocket URL |
| `--backfill-db` | `ATTIE_BACKFILL_DB` | `backfill.db` | Backfill state DB (file path for SQLite, or `postgres://` URI) |
| `--parallel-backfills` | `ATTIE_PARALLEL_BACKFILLS` | `300` | Number of active backfill jobs |
| `--parallel-downloads` | `ATTIE_PARALLEL_DOWNLOADS` | `64` | Maximum concurrent repo downloads/CAR walks (bounds streaming memory independently of active jobs) |
| `--metrics-addr` | `ATTIE_METRICS_ADDR` | `:9090` | Prometheus metrics / pprof address |
| `--log-file` | `ATTIE_LOG_FILE` | | Also write logs to this file |

On first run, bootes will create the `attie` database and all tables in ClickHouse automatically.

## Schema

The ClickHouse schema is in [`ingest/schema.sql`](ingest/schema.sql). Key tables:

- **`records`** -- all AT Protocol records, partitioned by month
- **`backlinks`** -- extracted DID/AT-URI references
- **`cursors`** -- firehose position tracking
- **`posts_by_time`** -- materialized view for time-ordered post queries

## License

MIT License

Copyright (c) 2025 Why

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
