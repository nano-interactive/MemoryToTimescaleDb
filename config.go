package mtsdb

import (
	"time"
)

type Config struct {
	TableName       string
	InsertDuration  time.Duration
	WorkerPoolSize  int
	BatchInsertSize int  // timescale batch insert length
	skipValidation  bool // internal, for unit tests
}
