package mtsdb

import "sync/atomic"

type Metric struct {
	fields []string
	count  atomic.Uint32
}
