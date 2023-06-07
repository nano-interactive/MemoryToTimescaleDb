package mtsdb

import (
	"context"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
)

func TestMetricCounter_Inc(t *testing.T) {
	assert := require.New(t)
	labels := []string{"url", "country"}
	counter, err := NewMetricCounter(context.TODO(), "test", MetricCounterConfig{}, labels...)
	assert.NoError(err)
	counter.Inc("https://example.com", "RS")
	counter.Inc("https://example.com", "GB")
	counter.Inc("https://example.com", "GB")
	counter.Inc("https://example.com2", "PL")
	counter.Inc("https://example.com2", "PL")
	counter.Inc("https://example.com2", "PL")

	// Assert that the appropriate count was added
	value, ok := counter.Get("https://example.com", "RS")
	assert.True(ok)
	assert.Equal(uint32(1), value)

	value, ok = counter.Get("https://example.com", "GB")
	assert.True(ok)
	assert.Equal(uint32(2), value)

	value, ok = counter.Get("https://example.com2", "PL")
	assert.True(ok)
	assert.Equal(uint32(3), value)

}

func TestMetricCounter_Add(t *testing.T) {
	assert := require.New(t)
	labels := []string{"url", "country"}
	counter, err := NewMetricCounter(context.TODO(), "test", MetricCounterConfig{}, labels...)
	assert.NoError(err)
	counter.Add(1, "https://example.com", "RS")
	counter.Add(2, "https://example.com", "GB")
	counter.Add(3, "https://example.com2", "PL")

	// Assert that the appropriate count was added
	value, ok := counter.Get("https://example.com", "RS")
	assert.True(ok)
	assert.Equal(uint32(1), value)

	value, ok = counter.Get("https://example.com", "GB")
	assert.True(ok)
	assert.Equal(uint32(2), value)

	value, ok = counter.Get("https://example.com2", "PL")
	assert.True(ok)
	assert.Equal(uint32(3), value)
}

func TestMetricCounter_Reset(t *testing.T) {
	assert := require.New(t)
	labels := []string{"name"}
	container := metricContainer{}
	container.Store(&sync.Map{})

	counter := metricCounter{
		ctx:       context.TODO(),
		name:      "test",
		labels:    sync.Map{},
		config:    MetricCounterConfig{},
		container: &container,
	}
	counter.labels.Store(0, labels[0])
	counter.labelsCnt.Store(1)

	// Add some values to the container
	err := counter.Add(2, "label1")
	assert.NoError(err)

	err = counter.Add(3, "label2")
	assert.NoError(err)

	oldMap := counter.reset()

	_, ok := counter.Get("label1")
	assert.False(ok)

	hash, err := hashLabels([]string{"label1"})
	assert.NoError(err)
	value, _ := oldMap.Load(hash)
	assert.NotNil(value)
	assert.Equal(uint32(2), value.(*MetricLabelValues).count.Load())
}

func TestMetricCounter_Write(t *testing.T) {
	assert := require.New(t)
	labels := []string{"name1", "name2"}
	counter, err := NewMetricCounter(context.TODO(), "test", MetricCounterConfig{}, labels...)
	assert.NoError(err)

	counter.Add(5, "value1", "value2")
	counter.Add(6, "value1", "value3")
	counter.Add(5, "value1", "value2")

	_, ok := counter.Get("value1", "value2")
	assert.True(ok)

	im := counter.Write()

	hash1, _ := hashLabels([]string{"value1", "value2"})
	hash2, _ := hashLabels([]string{"value1", "value3"})

	value1, _ := im.Container.Load(hash1)
	value2, _ := im.Container.Load(hash2)
	assert.NotNil(value1)
	assert.NotNil(value2)

	assert.Equal(uint32(10), value1.(*MetricLabelValues).count.Load())
	assert.Equal(uint32(6), value2.(*MetricLabelValues).count.Load())

	// current counter should be empty
	_, ok = counter.Get("value1", "value2")
	assert.False(ok)
}

func TestMetricCounter_Desc(t *testing.T) {
	assert := require.New(t)
	labels := []string{"name1", "name2"}
	counter, err := NewMetricCounter(context.TODO(), "test", MetricCounterConfig{Description: "test desc"}, labels...)
	assert.NoError(err)

	assert.Equal("test desc", counter.Desc())

}

func TestMetricCounter_ErrorCtx(t *testing.T) {
	assert := require.New(t)
	ctx, cancelFunc := context.WithCancel(context.Background())
	mc, err := NewMetricCounter(ctx, "test error", MetricCounterConfig{}, "url")

	err = mc.Inc("test")
	assert.NoError(err)

	cancelFunc()
	err = mc.Inc("test")
	assert.Error(err)
}

func TestMetricCounter_ErrorLabels(t *testing.T) {
	assert := require.New(t)

	mc, err := NewMetricCounter(context.TODO(), "test error", MetricCounterConfig{}, "url")

	err = mc.Inc("test")
	assert.NoError(err)

	err = mc.Inc("test", "test2")
	assert.Error(err)
}
