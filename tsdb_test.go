package tsdb

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestTSDB_WritePoints(t *testing.T) {
	db := New[int](10 * time.Minute)
	point1 := NewPoint[int]([]Tag{{Key: "cpu", Value: "#0"}}, time.Unix(100, 0), 100)
	point2 := NewPoint[int]([]Tag{{Key: "cpu", Value: "#0"}}, time.Unix(200, 0), 100)
	points := []Point[int]{point1, point2}

	err := db.WritePoints(points)
	assert.Nil(t, err)

	point := NewPoint[int]([]Tag{}, time.Unix(100, 0), 100)
	points = []Point[int]{point}
	err = db.WritePoints(points)
	assert.ErrorIs(t, err, ErrPointMissingTag)

	db.isClosed = true
	err = db.WritePoints(points)
	assert.ErrorIs(t, err, ErrDBClosed)
}

func TestTSDB_QueryPoints(t *testing.T) {
	db := New[int](10 * time.Minute)
	point1 := NewPoint[int]([]Tag{{Key: "cpu", Value: "#0"}}, time.Unix(100, 0), 100)
	point2 := NewPoint[int]([]Tag{{Key: "cpu", Value: "#1"}}, time.Unix(200, 0), 100)
	points := []Point[int]{point1, point2}

	err := db.WritePoints(points)
	assert.Nil(t, err)

	values := db.QueryPoints([]Tag{{Key: "cpu", Value: "#2"}}, time.Unix(100, 0), time.Unix(200, 0))
	assert.Empty(t, values)
	values = db.QueryPoints([]Tag{{Key: "cpu", Value: "#1"}}, time.Unix(100, 0), time.Unix(300, 0))
	for s, i := range values {
		assert.Equal(t, "cpu=#1", s)
		assert.Equal(t, []Value[int]{{200000000000, 100}}, i)
	}
}

func TestTSDB_GCAndStop(t *testing.T) {
	db := New[int](time.Millisecond)

	ti := time.Now().Add(10 * time.Minute)
	point1 := NewPoint[int]([]Tag{{Key: "cpu", Value: "#0"}}, time.Unix(100, 0), 100)
	point2 := NewPoint[int]([]Tag{{Key: "cpu", Value: "#0"}}, ti, 100)
	points := []Point[int]{point1, point2}

	err := db.WritePoints(points)
	assert.Nil(t, err)

	time.Sleep(2 * time.Millisecond)

	var seen bool
	for _, p := range db.store.partitions {
		p.mu.RLock()
		for s, e := range p.store {
			e.mu.RLock()
			if s == "cpu=#0" {
				assert.Len(t, e.values, 1)
				seen = true
			}
			e.mu.RUnlock()
		}
		p.mu.RUnlock()
	}
	assert.True(t, seen)

	db.Stop()
	assert.True(t, db.isClosed)
}
