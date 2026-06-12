package ingest

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// ConnectClickHouse creates a ClickHouse connection (native protocol for batch inserts)
func ConnectClickHouse(addr, user, password string) (driver.Conn, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{addr},
		Auth: clickhouse.Auth{
			Database: "attie",
			Username: user,
			Password: password,
		},
		Settings: clickhouse.Settings{
			"max_execution_time":              300,
			"max_partitions_per_insert_block": 0, // Unlimited - needed for backfill across many months
		},
		DialTimeout:          30 * time.Second,
		MaxOpenConns:         100,
		MaxIdleConns:         50,
		ConnMaxLifetime:      time.Hour,
		ConnOpenStrategy:     clickhouse.ConnOpenRoundRobin,
		BlockBufferSize:      10,
		MaxCompressionBuffer: 10 * 1024 * 1024,
	})
	if err != nil {
		return nil, fmt.Errorf("connect clickhouse: %w", err)
	}

	// Test connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := conn.Ping(ctx); err != nil {
		return nil, fmt.Errorf("ping clickhouse: %w", err)
	}

	return conn, nil
}

// ConnectClickHouseSQL creates a ClickHouse connection using database/sql (for queries)
func ConnectClickHouseSQL(addr, user, password string) (*sql.DB, error) {
	db := clickhouse.OpenDB(&clickhouse.Options{
		Addr: []string{addr},
		Auth: clickhouse.Auth{
			Database: "attie",
			Username: user,
			Password: password,
		},
		Settings: clickhouse.Settings{
			"max_execution_time": 300,
		},
		DialTimeout:     30 * time.Second,
		MaxOpenConns:    100,
		MaxIdleConns:    100,
		ConnMaxLifetime: time.Hour,
	})

	// Test connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("ping clickhouse: %w", err)
	}

	return db, nil
}
