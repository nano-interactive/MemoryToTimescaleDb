package mtsdb

import (
	"context"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestReset(t *testing.T) {
	assert := require.New(t)

	insertInc := 0

	tstConfig := Config{
		Size:      5,
		InsertSQL: "",
	}
	m := New(context.Background(), nil, tstConfig)
	m.bulkFunc = func(batch *pgx.Batch) {
		insertInc += batch.Len()
	}

	m.Inc("one")
	m.Inc("two")
	m.Inc("three")
	m.Inc("four")
	m.Inc("three")
	m.Inc("four")

	assert.Equal(0, insertInc, "bulk insert should not be called")
	assert.Equal(1, m.container["one"])
	assert.Equal(2, m.container["four"])

	m.reset()

	assert.Equal(0, insertInc, "bulk insert should not be called")
	assert.Equal(0, len(m.container))
	assert.Empty(m.container)

	m.Close()
}
