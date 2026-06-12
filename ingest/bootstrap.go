package ingest

import (
	"context"
	_ "embed"
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

//go:embed schema.sql
var schemaSQL string

// Bootstrap connects to ClickHouse and ensures the schema (database, tables,
// views) exists. The provided user must have sufficient privileges (CREATE
// DATABASE, CREATE TABLE, CREATE VIEW, INSERT, SELECT, ALTER). All DDL uses
// IF NOT EXISTS so this is safe to call repeatedly.
//
// addr is the primary address used for table/view DDL. nodes (optional) is a
// list of all ClickHouse node addresses — the CREATE DATABASE statement is
// executed on each node individually, since Replicated databases require each
// node to run CREATE DATABASE before DDL replication kicks in.
func Bootstrap(addr, user, password string, nodes []string) error {
	stmts := splitSQL(schemaSQL)
	if len(stmts) == 0 {
		return fmt.Errorf("no SQL statements found in schema")
	}

	// The first statement is CREATE DATABASE. Run it on every node so
	// Replicated database replication works across the cluster.
	createDBStmt := stmts[0]
	targets := nodes
	if len(targets) == 0 {
		targets = []string{addr}
	}
	for _, node := range targets {
		if err := execOnNode(node, user, password, createDBStmt); err != nil {
			return fmt.Errorf("bootstrap CREATE DATABASE on %s: %w", node, err)
		}
	}

	// Run remaining DDL (tables, views) on the primary address.
	// These replicate automatically via the Replicated database engine.
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{addr},
		Auth: clickhouse.Auth{
			Username: user,
			Password: password,
		},
		DialTimeout: 10 * time.Second,
	})
	if err != nil {
		return fmt.Errorf("bootstrap connect: %w", err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for _, stmt := range stmts[1:] {
		if err := conn.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("bootstrap schema exec: %w\nstatement: %s", err, stmt)
		}
	}

	return nil
}

// BootstrapReadonlyUser creates a read-only ClickHouse user with SELECT-only
// access to the attie database. The admin credentials are used to connect and
// execute the CREATE USER / GRANT statements. This is idempotent — the user is
// created with IF NOT EXISTS and the password is updated via ALTER if the user
// already exists.
func BootstrapReadonlyUser(addr, adminUser, adminPassword, readonlyUser, readonlyPassword string) error {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{addr},
		Auth: clickhouse.Auth{
			Username: adminUser,
			Password: adminPassword,
		},
		DialTimeout: 10 * time.Second,
	})
	if err != nil {
		return fmt.Errorf("readonly user bootstrap connect: %w", err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	stmts := []string{
		fmt.Sprintf("CREATE USER IF NOT EXISTS '%s' IDENTIFIED BY '%s'",
			escapeSingleQuotes(readonlyUser), escapeSingleQuotes(readonlyPassword)),
		fmt.Sprintf("ALTER USER '%s' IDENTIFIED BY '%s'",
			escapeSingleQuotes(readonlyUser), escapeSingleQuotes(readonlyPassword)),
		fmt.Sprintf("GRANT SELECT ON attie.* TO '%s'",
			escapeSingleQuotes(readonlyUser)),
	}

	for _, stmt := range stmts {
		if err := conn.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("readonly user bootstrap: %w\nstatement: %s", err, stmt)
		}
	}

	return nil
}

// escapeSingleQuotes escapes single quotes for use in ClickHouse SQL strings.
func escapeSingleQuotes(s string) string {
	return strings.ReplaceAll(s, "'", "\\'")
}

// execOnNode connects to a single ClickHouse node and executes a statement.
func execOnNode(addr, user, password, stmt string) error {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{addr},
		Auth: clickhouse.Auth{
			Username: user,
			Password: password,
		},
		DialTimeout: 10 * time.Second,
	})
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	return conn.Exec(ctx, stmt)
}

// splitSQL splits a SQL file into individual statements on semicolons,
// skipping empty statements and comments-only blocks.
func splitSQL(sql string) []string {
	raw := strings.Split(sql, ";")
	var stmts []string
	for _, s := range raw {
		s = strings.TrimSpace(s)
		// Skip empty or comment-only fragments
		if s == "" {
			continue
		}
		lines := strings.Split(s, "\n")
		hasContent := false
		for _, line := range lines {
			line = strings.TrimSpace(line)
			if line != "" && !strings.HasPrefix(line, "--") {
				hasContent = true
				break
			}
		}
		if hasContent {
			stmts = append(stmts, s)
		}
	}
	return stmts
}
