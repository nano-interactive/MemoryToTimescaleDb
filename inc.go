package mtsdb

import (
	"sync/atomic"
)

func (m *mtsdb) Inc(labels ...string) {
	if len(labels) == 0 {
		return
	}

	if m.ctx.Err() != nil { // no more inserts
		return
	}
	metric := Metric{
		fields: labels,
		count:  &atomic.Uint32{},
	}

	hashResult, err := m.generateHash(labels...)
	if err != nil {
		select { // non-blocking channel send
		case m.err <- err:
		default:
		}
		return
	}

	value, loaded := m.container.Load().LoadOrStore(hashResult, &metric)
	if !loaded {
		m.containerLen.Add(1)
	}

	if m.config.Size != 0 && m.containerLen.Load() >= m.config.Size {
		m.insert(m.reset())
	}

	value.(*Metric).count.Add(1)

}
