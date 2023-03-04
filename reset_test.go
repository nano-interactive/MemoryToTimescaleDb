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
	assert.Equal(uint64(1), m.container["one"].Load())
	assert.Equal(uint64(2), m.container["four"].Load())

	m.Reset()

	assert.Equal(0, insertInc, "bulk insert should not be called")
	assert.Equal(0, len(m.container))
	assert.Nil(m.container["one"])

	m.Close()
}
