package ingest

import "time"

// Record represents an AT Protocol record stored in ClickHouse
type Record struct {
	URI        string    `json:"uri"`
	DID        string    `json:"did"`
	Collection string    `json:"collection"`
	Rkey       string    `json:"rkey"`
	Record     string    `json:"record"` // JSON blob
	CreatedAt  time.Time `json:"created_at"`
	IndexedAt  time.Time `json:"indexed_at"`
	Seq        uint64    `json:"seq"`
}
