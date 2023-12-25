package mtsdb

import (
	"sync/atomic"
)

type MetricLabelValues struct {
	fields []string
	count  atomic.Uint32
}

type MetricInterface interface {
	Desc() string
	Write() *insertMetric
}
