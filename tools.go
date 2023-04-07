package mtsdb

import (
	"errors"
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	io_prometheus_client "github.com/prometheus/client_model/go"
	"strings"
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

	if len(mf) == 0 {
		return 0, MetricNotFound
	}
	for _, metric := range mf[0].GetMetric() {
		counter := 0
		for _, label := range labels {
			for _, metricLabel := range metric.GetLabel() {
				if label == metricLabel.GetValue() {
					counter++
					break
				}
			}
			if counter == len(labels) {
				return metric.GetCounter().GetValue(), nil
			}
		}
	}
	return 0, MetricNotFound
}

func (m *mtsdb) generateSql(mf *io_prometheus_client.MetricFamily) string {
	sql := "INSERT" + " INTO %s (%s) VALUES (%s)"

	var labels, values []string
	for _, metric := range mf.GetMetric() {
		counter := 1
		labels = make([]string, len(metric.GetLabel()))
		values = make([]string, len(metric.GetLabel()))
		for i, mLabel := range m.labels {
			for _, label := range metric.GetLabel() {
				if label.GetName() == mLabel {
					labels[i] = label.GetName()
					values[i] = fmt.Sprintf("$%d", counter)
					counter++
					break
				}
			}
		}
		break
	}
	labels = append(labels, "cnt")
	values = append(values, fmt.Sprintf("$%d", len(values)+1))

	return fmt.Sprintf(sql, m.config.TableName, strings.Join(labels, ","), strings.Join(values, ","))
}
