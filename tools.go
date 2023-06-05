package mtsdb

import (
	"fmt"
	"hash/fnv"
	"strings"
)

func hashLabels(labels []string) (uint32, error) {
	h := fnv.New32a()
	_, err := h.Write([]byte(strings.Join(labels, "")))
	if err != nil {
		return 0, err
	}
	return h.Sum32(), nil
}

func (m *mtsdb) generateSql(tableName string, labels []string) string {
	sql := "INSERT" + " INTO %s (%s) VALUES (%s)"

	values := make([]string, len(labels))
	for i := range labels {
		values[i] = fmt.Sprintf("$%d", i+1)
	}

	labels = append(labels, "cnt")
	values = append(values, fmt.Sprintf("$%d", len(labels)))

	return fmt.Sprintf(sql, tableName, strings.Join(labels, ","), strings.Join(values, ","))
}
