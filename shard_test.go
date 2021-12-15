package memtsdb

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestPool_updateIndex(t *testing.T) {
	pool := NewPool()

	pool.updateIndex(1, Tag{Key: "a", Value: "b"})
	assert.Equal(t, []int{1}, pool.index["a"]["b"])

	pool.updateIndex(2, Tag{Key: "a", Value: "b"})
	assert.Equal(t, []int{1, 2}, pool.index["a"]["b"])

	pool.updateIndex(3, Tag{Key: "a", Value: "c"})
	assert.Equal(t, []int{3}, pool.index["a"]["c"])
}

func TestPool_Insert(t *testing.T) {
	pool := NewPool()

	pool.Insert(Point{Tags: []Tag{{Key: "a", Value: "b"}, {Key: "c", Value: "d"}}, Field: 0})
	assert.Len(t, pool.points, 1)
	assert.Equal(t, []int{0}, pool.index["a"]["b"])
	assert.Equal(t, []int{0}, pool.index["c"]["d"])
}

func TestPool_Query(t *testing.T) {
	pool := NewPool()

	pool.Insert(Point{Tags: []Tag{{Key: "a", Value: "b"}, {Key: "c", Value: "d"}}, Field: 1})
	pool.Insert(Point{Tags: []Tag{{Key: "a", Value: "b"}, {Key: "e", Value: "f"}}, Field: 2})
	ps := pool.Query(Tag{Key: "a", Value: "b"})
	assert.Len(t, ps, 2)
	assert.Equal(t, int64(1), ps[0].Field)
	assert.Equal(t, int64(2), ps[1].Field)

	ps = pool.Query(Tag{Key: "g"})
	assert.Len(t, ps, 0)

	ps = pool.Query(Tag{Key: "a", Value: "c"})
	assert.Len(t, ps, 0)
}
