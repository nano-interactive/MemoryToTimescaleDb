package mtsdb

import (
	"errors"
	"github.com/prometheus/client_golang/prometheus"
)

var MetricNotFound = errors.New("metric not found")

func (m *mtsdb) fetchMetricValue(labels ...string) (float64, error) {
	r := prometheus.NewRegistry()
	err := r.Register(m.container.Load())
	if err != nil {
		return 0, err
	}

	mf, err := r.Gather()
	if err != nil {
		return 0, err
	}

	for _, metric := range mf[0].GetMetric() {
		counter := 0
		for i, label := range labels {
			if label == metric.GetLabel()[i].GetValue() {
				counter++
			}
			if counter == len(labels) {
				return metric.GetCounter().GetValue(), nil
			}
		}
	}
	return 0, MetricNotFound
}
