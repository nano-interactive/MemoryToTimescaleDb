package mtsdb

import (
	"errors"
	"fmt"
	"strings"
)

var MetricNotFound = errors.New("metric not found")

func (m *mtsdb) generateHash(labels ...string) (uint32, error) {
	_, err := m.hash32.Write([]byte(strings.Join(labels, "")))
	res := m.hash32.Sum32()
	m.hash32.Reset()
	if err != nil {
		return 0, err
	}
	return res, nil
}

func (m *mtsdb) fetchMetricValue(labels ...string) (uint32, error) {

	hashResult, err := m.generateHash(labels...)
	if err != nil {
		return 0, err
	}

	value, ok := m.container.Load().Load(hashResult)
	if !ok {
		return 0, MetricNotFound
	}
	mt := value.(*Metric)
	return mt.count.Load(), nil

}

func (m *mtsdb) generateSql() string {
	sql := "INSERT" + " INTO %s (%s) VALUES (%s)"

	labels := make([]string, len(m.labels))
	values := make([]string, len(m.labels))
	counter := 0
	for i, mLabel := range m.labels {
		counter++
		labels[i] = mLabel
		values[i] = fmt.Sprintf("$%d", counter)
	}

	labels = append(labels, "cnt")
	values = append(values, fmt.Sprintf("$%d", counter+1))

	return fmt.Sprintf(sql, m.config.TableName, strings.Join(labels, ","), strings.Join(values, ","))
}
