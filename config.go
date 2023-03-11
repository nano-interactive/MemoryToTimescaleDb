package mtsdb

import "time"

type Config struct {
	Size           int
	InsertSQL      string
	InsertDuration time.Duration
	UseFnvHash     bool
}
