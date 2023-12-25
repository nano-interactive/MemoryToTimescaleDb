package mtsdb

import (
	"bytes"
	"hash/fnv"
	"strconv"
	"strings"
	"unsafe"
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

	bb := bytes.NewBuffer(nil)
	bb.WriteString("INSERT ")
	bb.WriteString("INTO ")
	bb.WriteString(tableName)
	bb.WriteString(" (")
	for _, label := range labels {
		bb.WriteString(label)
		bb.WriteRune(',')
	}
	bb.WriteString("cnt) VALUES (")

	for i := 0; i < len(labels); i++ {
		bb.WriteRune('$')
		bb.WriteString(strconv.FormatInt(int64(i+1), 10))
		bb.WriteRune(',')
	}
	bb.WriteString("$")
	bb.WriteString(strconv.FormatInt(int64(len(labels)+1), 10))
	bb.WriteString(")")

	return unsafe.String(unsafe.SliceData(bb.Bytes()), bb.Len())
}
