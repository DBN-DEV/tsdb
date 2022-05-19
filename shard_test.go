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
	p := newPartition[int]()

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

func TestShard_GetPartition(t *testing.T) {
	s := newShard[int]()
	p := s.getPartitions("a")
	assert.NotNil(t, p)
}

func TestShard_WriteMulti(t *testing.T) {
	s := newShard[int]()
	s.writeMulti(map[string][]value[int]{"a": {{100, 200}, {300, 400}}, "c": {{300, 400}}})

	p := s.getPartitions("a")
	assert.Len(t, p.store, 1)
	assert.Len(t, p.store["a"].values, 2)
	p = s.getPartitions("c")
	assert.Len(t, p.store, 1)
	assert.Len(t, p.store["c"].values, 1)
}
