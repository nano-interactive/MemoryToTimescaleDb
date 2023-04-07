package mtsdb

import (
	"context"
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	io_prometheus_client "github.com/prometheus/client_model/go"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

func (m *mtsdb) insert(counterVec prometheus.CounterVec) {
	defer m.wg.Done()
	m.wg.Add(1)

	r := prometheus.NewRegistry()
	err := r.Register(counterVec)
	if err != nil {
		m.raiseError(err)
		return
	}

	mf, err := r.Gather()
	if err != nil {
		m.raiseError(err)
		return
	}

	if len(mf) == 0 {
		return
	}

	batch := new(pgx.Batch)

	sql := m.generateSql(mf[0])

	for _, metric := range mf[0].GetMetric() {
		values := make([]any, len(metric.GetLabel()))
		for i, label := range metric.GetLabel() {
			values[i] = label.GetValue()
		}
		batch.Queue(sql, values...)

		if batch.Len() >= m.config.BatchInsertSize {
			m.wg.Add(1) // m.wg.Done() is on sendBatch
			m.job <- *batch
			batch = &pgx.Batch{}
		}
	}

	if batch.Len() > 0 {
		m.wg.Add(1) // m.wg.Done() is on sendBatch
		m.job <- *batch
	}
}

func (m *mtsdb) raiseError(err error) {
	select { // non-blocking channel send
	case m.err <- err:
	default:
	}

}

func (m *mtsdb) generateSql(mf *io_prometheus_client.MetricFamily) string {
	sql := "INSERT" + " INTO %s (%s) VALUES (%s)"

	var labels, values []string

	for _, metric := range mf.GetMetric() {
		labels = make([]string, len(metric.GetLabel()))
		values = make([]string, len(metric.GetLabel()))
		for i, label := range metric.GetLabel() {
			labels[i] = label.GetName()
			values[i] = "$1"
		}
		break
	}
	return fmt.Sprintf(sql, m.config.TableName, strings.Join(labels, ","), strings.Join(values, ","))
}

// bulk insert
func (m *mtsdb) sendBatch(batch *pgx.Batch) {
	defer m.wg.Done()

	tm := time.Now().UnixMilli()
	br := m.pool.SendBatch(context.Background(), batch)
	defer func(br pgx.BatchResults) {
		err := br.Close()
		if err != nil {
			select { // non-blocking channel send
			case m.err <- err:
			default:
			}

		}
	}(br)
	_, err := br.Exec()
	if err != nil {
		select { // non-blocking channel send
		case m.err <- err:
		default:
		}
		return
	}
	m.MetricInserts.Add(uint64(batch.Len()))
	m.MetricDurationMs.Add(uint64(time.Now().UnixMilli() - tm))
}
