package memtsdb

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestShard_updateIndex(t *testing.T) {
	s := NewShard()

	s.updateIndex(1, Tag{Key: "a", Value: "b"})
	assert.Equal(t, []int{1}, s.index["a"]["b"])

	s.updateIndex(2, Tag{Key: "a", Value: "b"})
	assert.Equal(t, []int{1, 2}, s.index["a"]["b"])

	s.updateIndex(3, Tag{Key: "a", Value: "c"})
	assert.Equal(t, []int{3}, s.index["a"]["c"])
}

func TestShard_Insert(t *testing.T) {
	s := NewShard()

	s.Insert(Point{Tags: []Tag{{Key: "a", Value: "b"}, {Key: "c", Value: "d"}}, Field: 0})
	assert.Len(t, s.points, 1)
	assert.Equal(t, []int{0}, s.index["a"]["b"])
	assert.Equal(t, []int{0}, s.index["c"]["d"])
}

func TestShard_Query(t *testing.T) {
	s := NewShard()

	s.Insert(Point{Tags: []Tag{{Key: "a", Value: "b"}}, Field: 1, Time: time.Unix(1, 0)})
	s.Insert(Point{Tags: []Tag{{Key: "a", Value: "b"}}, Field: 2, Time: time.Unix(2, 0)})
	s.Insert(Point{Tags: []Tag{{Key: "a", Value: "b"}}, Field: 3, Time: time.Unix(3, 0)})
	s.Insert(Point{Tags: []Tag{{Key: "a", Value: "b"}}, Field: 10, Time: time.Unix(10, 0)})

	ps := s.Query(Tag{Key: "a", Value: "b"}, time.Unix(2, 0), time.Unix(3, 0))
	assert.Len(t, ps, 2)
	assert.Equal(t, int64(2), ps[0].Field)
	assert.Equal(t, int64(3), ps[1].Field)

	ps = s.Query(Tag{Key: "g"}, time.Unix(1, 0), time.Unix(2, 0))
	assert.Empty(t, ps)

	ps = s.Query(Tag{Key: "a", Value: "c"}, time.Unix(1, 0), time.Unix(2, 0))
	assert.Empty(t, ps)
}

func TestShard_Clear(t *testing.T) {
	s := NewShard()

	s.Insert(Point{Tags: []Tag{{Key: "a", Value: "b"}}, Field: 1, Time: time.Unix(1, 0)})
	s.Insert(Point{Tags: []Tag{{Key: "a", Value: "b"}}, Field: 2, Time: time.Unix(2, 0)})
	s.Insert(Point{Tags: []Tag{{Key: "a", Value: "b"}}, Field: 3, Time: time.Unix(3, 0)})

	s.Clear()

	assert.Empty(t, s.points)
	assert.Empty(t, s.index)
}
