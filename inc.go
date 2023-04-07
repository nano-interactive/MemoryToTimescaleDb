package mtsdb

func (m *mtsdb) Inc(labels ...string) {
	if len(labels) == 0 {
		return
	}

	if m.ctx.Err() != nil { // no more inserts
		return
	}

	m.container.Load().WithLabelValues(labels...).Inc()

}
