package mtsdb

import (
	"github.com/prometheus/client_golang/prometheus"
)

func (m *mtsdb) reset() prometheus.CounterVec {
	//counterVec := prometheus.NewCounterVec(prometheus.CounterOpts{
	//	Name: "mtsdb",
	//}, []string{})
	//return //m.container.Swap(counterVec)
	return *m.container.Load()
}
