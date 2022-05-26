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

func TestEntry_RemoveBefore(t *testing.T) {
	var e entry[int]
	e.add([]value[int]{{100, 100}, {200, 200}, {300, 300}})
	e.removeBefore(200)
	assert.Equal(t, []value[int]{{200, 200}, {300, 300}}, e.values)
}

func TestEntry_ValuesBetween(t *testing.T) {
	var e entry[int]
	e.add([]value[int]{{50, 50}, {100, 100}, {200, 200}, {300, 300}, {400, 400}})
	values := e.valuesBetween(100, 300)
	assert.Equal(t, []value[int]{{100, 100}, {200, 200}, {300, 300}}, values)
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

func TestPartition_RemoveBefore(t *testing.T) {
	p := newPartition[int]()

	p.write("a", []value[int]{{100, 200}})
	p.removeBefore(200)
	assert.Empty(t, p.store["a"].values)

	// 检查 slice 和 map 缩容
	p.removeBefore(200)
	assert.Empty(t, p.store)
}

func TestPartition_ValuesBetween(t *testing.T) {
	p := newPartition[int]()

	p.write("a", []value[int]{{50, 50}, {100, 100}, {200, 200}, {300, 300}, {400, 400}})
	values := p.valuesBetween("a", 100, 300)
	assert.Equal(t, []value[int]{{100, 100}, {200, 200}, {300, 300}}, values)

	values = p.valuesBetween("b", 400, 600)
	assert.Empty(t, values)
}

func TestShard_RemoveBefore(t *testing.T) {
	s := newShard[int]()
	s.writeMulti(map[string][]value[int]{"a": {{100, 200}, {300, 400}}, "c": {{300, 400}}})
	s.removeBefore(400)

	for _, p := range s.partitions {
		for _, e := range p.store {
			assert.Empty(t, e.values)
		}
	}
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
