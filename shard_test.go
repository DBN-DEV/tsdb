package tsdb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewEntry(t *testing.T) {
	e := newEntry([]value[int]{{100, 200}})
	assert.Len(t, e.values, 1)
}

func TestEntry_Add(t *testing.T) {
	var e entry[int]
	e.add([]value[int]{{200, 400}})
	assert.Len(t, e.values, 1)
}

func TestPartition_Write(t *testing.T) {
	p := partition[int]{store: make(map[string]*entry[int])}

	// 新 key 写入
	p.write("a", []value[int]{{100, 200}})
	expect := make(map[string]*entry[int])
	expect["a"] = newEntry([]value[int]{{100, 200}})
	assert.Equal(t, expect, p.store)

	// 写入已经存在的 key
	p.write("a", []value[int]{{300, 400}})
	expect = make(map[string]*entry[int])
	expect["a"] = newEntry([]value[int]{{100, 200}, {300, 400}})
	assert.Equal(t, expect, p.store)
}
