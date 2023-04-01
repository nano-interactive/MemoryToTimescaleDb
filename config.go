package mtsdb

import (
	"hash"
	"time"
)

type Hasher interface {
	hash.Hash32
	hash.Hash
}

type Config struct {
	Size            uint64
	InsertSQL       string
	InsertDuration  time.Duration
	Hasher          func() Hasher
	WorkerPoolSize  int
	BatchInsertSize int  // timescale batch insert length
	skipValidation  bool // internal, for unit tests
}
