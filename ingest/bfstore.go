package ingest

import (
	"fmt"
	"strings"
	"time"

	"github.com/bluesky-social/indigo/backfill"
	"gorm.io/driver/postgres"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// PumpProgress tracks the progress of the initial repo pump
type PumpProgress struct {
	ID          uint   `gorm:"primaryKey"`
	Name        string `gorm:"uniqueIndex"` // e.g., "repo_pump"
	Cursor      string // Last cursor position
	TotalPumped int64  // Total repos pumped so far
	Completed   bool   // Whether the pump finished
	UpdatedAt   time.Time
}

// NewBackfillStore creates a new backfill store using indigo's Gormstore.
// The connStr is either a file path (for SQLite) or a postgres:// URI.
func NewBackfillStore(connStr string) (*backfill.Gormstore, *gorm.DB, error) {
	var db *gorm.DB
	var err error

	if isPostgresURI(connStr) {
		db, err = gorm.Open(postgres.Open(connStr), &gorm.Config{
			Logger: logger.Default.LogMode(logger.Silent),
		})
	} else {
		dsn := connStr + "?_journal_mode=WAL&_busy_timeout=30000&cache=shared&_txlock=immediate"
		db, err = gorm.Open(sqlite.Open(dsn), &gorm.Config{
			Logger: logger.Default.LogMode(logger.Silent),
		})
		if err == nil {
			db.Exec("PRAGMA synchronous=NORMAL")
			db.Exec("PRAGMA cache_size=-64000")
		}
	}
	if err != nil {
		return nil, nil, fmt.Errorf("open backfill db: %w", err)
	}

	// Auto-migrate tables
	if err := db.AutoMigrate(&backfill.GormDBJob{}, &PumpProgress{}); err != nil {
		return nil, nil, err
	}

	store := backfill.NewGormstore(db)
	return store, db, nil
}

func isPostgresURI(s string) bool {
	return strings.HasPrefix(s, "postgres://") || strings.HasPrefix(s, "postgresql://")
}
