package mtsdb

import (
	"context"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestGenerateSql(t *testing.T) {
	t.Parallel()
	assert := require.New(t)

	type testData struct {
		labels    []string
		tableName string
		result    string
	}

	metrics := []testData{
		{
			labels:    []string{"one"},
			tableName: "test",
			result:    "INSERT" + " INTO test (one,cnt) VALUES ($1,$2)",
		},
		{
			labels:    []string{"one", "two"},
			tableName: "test2",
			result:    "INSERT" + " INTO test2 (one,two,cnt) VALUES ($1,$2,$3)",
		}, {
			labels:    []string{"one", "two", "three", "four", "five"},
			tableName: "test3",
			result:    "INSERT" + " INTO test3 (one,two,three,four,five,cnt) VALUES ($1,$2,$3,$4,$5,$6)",
		},
	}

	for _, metric := range metrics {
		tstConfig := DefaultConfig()
		tstConfig.InsertDuration = 10 * time.Minute
		ctx := context.Background()
		m, err := newMtsdb(ctx, &pgxpool.Pool{}, tstConfig)
		assert.NoError(err)

		counter, err := NewMetricCounter(ctx, "testCounter", MetricCounterConfig{
			TableName:   metric.tableName,
			Description: "desc",
		}, metric.labels...)

		assert.NoError(err)

		m.MustRegister(counter)

		_ = counter.Inc(metric.labels...)

		assert.Equal(metric.result, m.generateSql(metric.tableName, metric.labels))

	}

}
