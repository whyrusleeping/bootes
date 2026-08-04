package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/urfave/cli/v3"
	"github.com/whyrusleeping/bootes/ingest"
	"go.opentelemetry.io/otel"
	promexporter "go.opentelemetry.io/otel/exporters/prometheus"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

var sigCh = make(chan os.Signal, 1)

func main() {
	// Ignore SIGPIPE so broken pipes (e.g. tee dying) don't kill us
	signal.Ignore(syscall.SIGPIPE)
	// Capture signals immediately, before any setup
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	app := &cli.Command{
		Name:  "ingest",
		Usage: "AT Protocol firehose ingester for ClickHouse",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "clickhouse",
				Value:   "localhost:9000",
				Usage:   "ClickHouse address (native protocol)",
				Sources: cli.EnvVars("ATTIE_CLICKHOUSE"),
			},
			&cli.StringFlag{
				Name:    "vals",
				Usage:   "Store into a vals cluster instead of ClickHouse: comma list of id@host:port data nodes",
				Sources: cli.EnvVars("ATTIE_VALS"),
			},
			&cli.BoolFlag{
				Name:    "disable-backfill",
				Usage:   "Firehose-only: skip the repo pump and backfiller",
				Sources: cli.EnvVars("ATTIE_DISABLE_BACKFILL"),
			},
			&cli.StringFlag{
				Name:    "shard",
				Usage:   "Backfill only repos in shard n of K (\"n/K\"): partitions the repo pump by DID hash so multiple instances split the network. Default: all repos.",
				Sources: cli.EnvVars("ATTIE_SHARD"),
			},
			&cli.BoolFlag{
				Name:    "disable-firehose",
				Usage:   "Skip the live firehose subscription (for backfill-only worker shards; run exactly one instance WITHOUT this to consume the firehose)",
				Sources: cli.EnvVars("ATTIE_DISABLE_FIREHOSE"),
			},
			&cli.StringSliceFlag{
				Name:    "clickhouse-bootstrap-nodes",
				Usage:   "All ClickHouse node addresses for bootstrap (CREATE DATABASE runs on each)",
				Sources: cli.EnvVars("ATTIE_CLICKHOUSE_BOOTSTRAP_NODES"),
			},
			&cli.StringFlag{
				Name:    "backfill-db",
				Value:   "backfill.db",
				Usage:   "Backfill state DB: file path for SQLite or postgres:// URI for Postgres",
				Sources: cli.EnvVars("ATTIE_BACKFILL_DB"),
			},
			&cli.StringFlag{
				Name:    "relay",
				Value:   "",
				Usage:   "Relay WebSocket URL (default: wss://bsky.network)",
				Sources: cli.EnvVars("ATTIE_RELAY"),
			},
			&cli.IntFlag{
				Name:    "parallel-backfills",
				Value:   0,
				Usage:   "Number of active backfill jobs (default: 300)",
				Sources: cli.EnvVars("ATTIE_PARALLEL_BACKFILLS"),
			},
			&cli.IntFlag{
				Name:    "parallel-downloads",
				Value:   0,
				Usage:   "Maximum concurrent repo downloads/CAR walks (default: 64; bounds memory while jobs remain parallel)",
				Sources: cli.EnvVars("ATTIE_PARALLEL_DOWNLOADS"),
			},
			&cli.StringFlag{
				Name:    "metrics-addr",
				Value:   ":9090",
				Usage:   "Address for Prometheus metrics endpoint",
				Sources: cli.EnvVars("ATTIE_METRICS_ADDR"),
			},
			&cli.StringFlag{
				Name:    "clickhouse-user",
				Value:   "attie",
				Usage:   "ClickHouse username",
				Sources: cli.EnvVars("ATTIE_CLICKHOUSE_USER"),
			},
			&cli.StringFlag{
				Name:    "clickhouse-password",
				Value:   "attie",
				Usage:   "ClickHouse password",
				Sources: cli.EnvVars("ATTIE_CLICKHOUSE_PASSWORD"),
			},
			&cli.StringFlag{
				Name:    "clickhouse-readonly-user",
				Value:   "attie-readonly",
				Usage:   "ClickHouse read-only username (created during bootstrap)",
				Sources: cli.EnvVars("ATTIE_CLICKHOUSE_READONLY_USER"),
			},
			&cli.StringFlag{
				Name:    "clickhouse-readonly-password",
				Value:   "",
				Usage:   "ClickHouse read-only password (created during bootstrap)",
				Sources: cli.EnvVars("ATTIE_CLICKHOUSE_READONLY_PASSWORD"),
			},
			&cli.StringFlag{
				Name:    "log-file",
				Value:   "",
				Usage:   "Path to log file (logs go to both stdout and file)",
				Sources: cli.EnvVars("ATTIE_LOG_FILE"),
			},
			&cli.DurationFlag{
				Name:    "shutdown-timeout",
				Value:   30 * time.Second,
				Usage:   "Maximum total graceful shutdown time before exiting with loss diagnostics",
				Sources: cli.EnvVars("ATTIE_SHUTDOWN_TIMEOUT"),
			},
		},
		Action: run,
	}

	if err := app.Run(context.Background(), os.Args); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

// parseShard parses a "n/K" shard spec. Empty -> (0, 1) meaning "all repos".
// Requires 0 <= n < K.
func parseShard(s string) (int, int, error) {
	if s == "" {
		return 0, 1, nil
	}
	nStr, kStr, ok := strings.Cut(s, "/")
	if !ok {
		return 0, 0, fmt.Errorf("expected n/K, got %q", s)
	}
	n, err := strconv.Atoi(strings.TrimSpace(nStr))
	if err != nil {
		return 0, 0, fmt.Errorf("bad shard index %q", nStr)
	}
	k, err := strconv.Atoi(strings.TrimSpace(kStr))
	if err != nil {
		return 0, 0, fmt.Errorf("bad shard count %q", kStr)
	}
	if k < 1 || n < 0 || n >= k {
		return 0, 0, fmt.Errorf("require 0 <= n < K, got %d/%d", n, k)
	}
	return n, k, nil
}

// parseValsNodes parses "1@host:port,2@host:port,..." into the valsgo node map.
func parseValsNodes(list string) (map[uint32]string, error) {
	nodes := make(map[uint32]string)
	for _, part := range strings.Split(list, ",") {
		id, addr, ok := strings.Cut(part, "@")
		if !ok {
			return nil, fmt.Errorf("expected id@host:port, got %q", part)
		}
		n, err := strconv.ParseUint(id, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("bad node id %q", id)
		}
		nodes[uint32(n)] = addr
	}
	return nodes, nil
}

func run(ctx context.Context, cmd *cli.Command) error {
	var logWriter io.Writer = os.Stdout
	if logFile := cmd.String("log-file"); logFile != "" {
		f, err := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			return fmt.Errorf("failed to open log file: %w", err)
		}
		defer f.Close()
		logWriter = io.MultiWriter(os.Stdout, f)
	}
	logger := slog.New(slog.NewTextHandler(logWriter, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	clickhouseAddr := cmd.String("clickhouse")
	clickhouseUser := cmd.String("clickhouse-user")
	clickhousePassword := cmd.String("clickhouse-password")
	clickhouseReadonlyUser := cmd.String("clickhouse-readonly-user")
	clickhouseReadonlyPassword := cmd.String("clickhouse-readonly-password")
	backfillDB := cmd.String("backfill-db")
	relayURL := cmd.String("relay")
	parallelBackfills := cmd.Int("parallel-backfills")
	parallelDownloads := cmd.Int("parallel-downloads")
	metricsAddr := cmd.String("metrics-addr")
	shutdownTimeout := cmd.Duration("shutdown-timeout")
	if shutdownTimeout <= 0 {
		return fmt.Errorf("--shutdown-timeout must be positive")
	}

	// Set up Prometheus metrics exporter via OTel SDK
	exporter, err := promexporter.New()
	if err != nil {
		return fmt.Errorf("failed to create prometheus exporter: %w", err)
	}
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(exporter))
	defer provider.Shutdown(context.Background())
	otel.SetMeterProvider(provider)

	metrics := ingest.NewMetrics()

	// Start metrics HTTP server (uses DefaultServeMux so net/http/pprof registers automatically)
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		logger.Info("metrics server starting", "addr", metricsAddr)
		if err := http.ListenAndServe(metricsAddr, nil); err != nil && err != http.ErrServerClosed {
			logger.Error("metrics server error", "error", err)
		}
	}()

	var (
		writer         ingest.RecordSink
		backlinkWriter ingest.BacklinkSink
		deleteQueue    ingest.DeleteSink
		cursors        ingest.CursorStore
		conn           driver.Conn
	)
	if valsNodes := cmd.String("vals"); valsNodes != "" {
		nodes, err := parseValsNodes(valsNodes)
		if err != nil {
			return fmt.Errorf("--vals: %w", err)
		}
		store, err := ingest.NewValsStore(nodes, logger)
		if err != nil {
			return fmt.Errorf("vals store: %w", err)
		}
		logger.Info("vals connected", "nodes", len(nodes))
		writer, backlinkWriter, deleteQueue, cursors = store, store, store, store
	} else {
		// Bootstrap ClickHouse database, user, and schema if needed
		clickhouseNodes := cmd.StringSlice("clickhouse-bootstrap-nodes")
		if err := ingest.Bootstrap(clickhouseAddr, clickhouseUser, clickhousePassword, clickhouseNodes); err != nil {
			logger.Warn("clickhouse bootstrap failed (may already be set up)", "error", err)
		} else {
			logger.Info("clickhouse bootstrap complete")
		}

		// Create read-only user if credentials are provided
		if clickhouseReadonlyPassword != "" {
			if err := ingest.BootstrapReadonlyUser(clickhouseAddr, clickhouseUser, clickhousePassword, clickhouseReadonlyUser, clickhouseReadonlyPassword); err != nil {
				logger.Warn("clickhouse read-only user bootstrap failed", "error", err)
			} else {
				logger.Info("clickhouse read-only user bootstrapped", "user", clickhouseReadonlyUser)
			}
		}

		// Connect to ClickHouse (native protocol for batch inserts)
		conn, err = ingest.ConnectClickHouse(clickhouseAddr, clickhouseUser, clickhousePassword)
		if err != nil {
			return fmt.Errorf("failed to connect to clickhouse: %w", err)
		}
		logger.Info("clickhouse connected", "addr", clickhouseAddr)

		writer = ingest.NewBatchWriter(conn, logger, metrics)
		backlinkWriter = ingest.NewBacklinkWriter(conn, logger, metrics)
		deleteQueue = ingest.NewDeleteQueue(conn, logger, metrics)
	}

	shardN, shardK, err := parseShard(cmd.String("shard"))
	if err != nil {
		return fmt.Errorf("--shard: %w", err)
	}

	ingester, err := ingest.NewIngester(ingest.Config{
		RelayURL:          relayURL,
		BackfillDBPath:    backfillDB,
		ParallelBackfills: parallelBackfills,
		ParallelDownloads: parallelDownloads,
		ClickHouseConn:    conn,
		Metrics:           metrics,
		Cursors:           cursors,
		DisableBackfill:   cmd.Bool("disable-backfill"),
		DisableFirehose:   cmd.Bool("disable-firehose"),
		ShardN:            shardN,
		ShardK:            shardK,
	}, writer, backlinkWriter, deleteQueue, logger)
	if err != nil {
		return fmt.Errorf("failed to create ingester: %w", err)
	}

	// Run ingester with signal handling
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- ingester.Run(ctx)
	}()

	logger.Info("firehose ingester started", "metrics", metricsAddr)

	runExited := false
	select {
	case err := <-errCh:
		runExited = true
		// Unexpected error — still do bounded graceful shutdown.
		if err != nil && !errors.Is(err, context.Canceled) {
			logger.Error("ingester exited with error", "error", err)
		}
	case sig := <-sigCh:
		logger.Info("received signal; stopping admission and beginning bounded shutdown",
			"signal", sig, "shutdown_timeout", shutdownTimeout,
			"force_hint", "send another signal to force quit")
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer shutdownCancel()
	ingester.BeginShutdown(shutdownCtx)
	cancel() // immediately stop firehose and repo-pump admission
	if !runExited {
		select {
		case err := <-errCh:
			if err != nil && !errors.Is(err, context.Canceled) {
				logger.Warn("ingester run stopped during shutdown", "error", err)
			}
		case sig := <-sigCh:
			logger.Warn("received second signal, force quitting", "signal", sig)
			os.Exit(1)
			return nil
		case <-shutdownCtx.Done():
			// CloseContext owns the producer-quiescence diagnostics and quantitative
			// vals snapshot. Invoke it with the already-expired shared context; it
			// must not close sinks or save the cursor.
			if err := ingester.CloseContext(shutdownCtx); err != nil {
				return fmt.Errorf("producer quiescence: %w", err)
			}
			return fmt.Errorf("producer quiescence: %w", shutdownCtx.Err())
		}
	}
	closeCh := make(chan error, 1)
	go func() { closeCh <- ingester.CloseContext(shutdownCtx) }()
	select {
	case err := <-closeCh:
		if err != nil {
			return fmt.Errorf("shutdown incomplete: %w", err)
		}
		return nil
	case sig := <-sigCh:
		logger.Warn("received second signal, force quitting", "signal", sig)
		os.Exit(1)
		return nil // unreachable; keeps the signal branch explicit to the compiler
	case <-shutdownCtx.Done():
		// CloseContext observes this same deadline and emits the quantitative
		// summary before returning. Give it a small, fixed reporting grace; this
		// remains a process-level guard against a buggy sink implementation.
		select {
		case err := <-closeCh:
			if err != nil {
				return fmt.Errorf("shutdown incomplete: %w", err)
			}
			return nil
		case <-time.After(time.Second):
			logger.Error("shutdown deadline expired; force exiting", "error", shutdownCtx.Err())
			return fmt.Errorf("shutdown deadline: %w", shutdownCtx.Err())
		}
	}
}
