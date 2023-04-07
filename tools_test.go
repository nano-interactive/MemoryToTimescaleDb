package mtsdb

import (
	"context"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestGenerateSql(t *testing.T) {
	assert := require.New(t)

	type testData struct {
		labels []string
		result string
	}

	metrics := []testData{
		{
			labels: []string{"one"},
			result: "INSERT" + " INTO test (one) VALUES ($1)",
		},
		{
			labels: []string{"one", "two"},
			result: "INSERT" + " INTO test (one,two) VALUES ($1,$2)",
		}, {
			labels: []string{"one", "two", "three", "four", "five"},
			result: "INSERT" + " INTO test (one,two,three,four,five) VALUES ($1,$2,$3,$4,$5)",
		},
	}

	for _, metric := range metrics {
		tstConfig := DefaultConfig()
		tstConfig.InsertDuration = 10 * time.Minute
		tstConfig.TableName = "test"
		m, err := newMtsdb(context.Background(), &pgxpool.Pool{}, tstConfig, metric.labels...)
		assert.NoError(err)
		m.Inc(metric.labels...)
		r := prometheus.NewRegistry()
		err = r.Register(*m.container.Load())
		assert.NoError(err)

		mf, err := r.Gather()
		assert.NoError(err)

		assert.Equal(metric.result, m.generateSql(mf[0]))

		//_ = m.Close()

	}

}
