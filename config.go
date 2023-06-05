package mtsdb

import (
	"time"
)

type Config struct {
	InsertDuration  time.Duration
	WorkerPoolSize  int
	BatchInsertSize int  // timescale batch insert length
	skipValidation  bool // internal, for unit tests
}

func DefaultConfig() Config {
	return Config{
		InsertDuration:  1 * time.Minute,
		WorkerPoolSize:  5,
		BatchInsertSize: 1_000,
	}
}
