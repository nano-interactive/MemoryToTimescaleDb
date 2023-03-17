package mtsdb

import (
	"context"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestIncEmptyString(t *testing.T) {
	assert := require.New(t)

	insertInc := 0

	tstConfig := Config{
		Size:      5,
		InsertSQL: "",
	}
	m := New(context.Background(), nil, tstConfig)
	m.bulkFunc = func(batch *pgx.Batch) {
	}

	m.Inc("")
	m.Inc("")

	assert.Equal(0, insertInc, "bulk insert should not be called")
	assert.Equal(0, len(m.container))
	m.Close()
}
