package mtsdb

import (
	"context"
	"sync"
	"sync/atomic"
)

var _ Counter = &metricCounter{}

type MetricCounterConfig struct {
	TableName   string
	Description string
}

type Counter interface {
	MetricInterface
	Inc(labelValues ...string)
	Add(value uint32, labels ...string)
	Get(labelValues ...string) (uint32, bool)
}

type metricCounter struct {
	ctx       context.Context
	container *metricContainer
	name      string
	labels    sync.Map
	config    MetricCounterConfig
}

func NewMetricCounter(ctx context.Context, name string, metricCounterConfig MetricCounterConfig, labels ...string) (Counter, error) {
	mc := metricContainer{}
	mc.Store(&sync.Map{})

	m := metricCounter{
		ctx:       ctx,
		name:      name,
		labels:    sync.Map{},
		config:    metricCounterConfig,
		container: &mc,
	}
	for i, label := range labels {
		m.labels.Store(i, label)
	}

	return &m, nil
}

func (m *metricCounter) Inc(labelValues ...string) {
	m.Add(1, labelValues...)
}

func (m *metricCounter) Add(count uint32, labelValues ...string) {
	if len(labelValues) == 0 {
		return
	}

	if m.ctx.Err() != nil { // no more inserts
		return
	}
	metricLabelValues := MetricLabelValues{
		fields: labelValues,
		count:  atomic.Uint32{},
	}

	hashResult, err := hashLabels(labelValues)
	if err != nil { // TODO handle error
		//select { // non-blocking channel send
		//case m.err <- err:
		//default:
		//}
		return
	}

	value, _ := m.container.Load().LoadOrStore(hashResult, &metricLabelValues)

	value.(*MetricLabelValues).count.Add(count)
}

func (m *metricCounter) Desc() string {
	return m.config.Description
}

func (m *metricCounter) Write() *insertMetric {
	oldContainer := m.reset()
	labels := make([]string, 0)
	m.labels.Range(func(key, value any) bool {
		labels = append(labels, value.(string))
		return true
	})
	return &insertMetric{
		TableName: m.config.TableName,
		Container: oldContainer,
		Labels:    labels,
	}
}

func (m *metricCounter) Get(labelValues ...string) (uint32, bool) {
	hash, err := hashLabels(labelValues)
	if err != nil {
		return 0, false
	}
	value, ok := m.container.Load().Load(hash)
	count := uint32(0)
	if ok {
		count = value.(*MetricLabelValues).count.Load()
	}
	return count, ok
}

func (m *metricCounter) reset() *sync.Map {
	return m.container.Swap(&sync.Map{})
}
