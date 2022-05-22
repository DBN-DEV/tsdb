package tsdb

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestPoint_SortTag(t *testing.T) {
	p := NewPoint[int]([]Tag{{"b", "b"}, {"a", "a"}}, time.Unix(100, 0), 100)
	p.sortTags()

	assert.Equal(t, []Tag{{"a", "a"}, {"b", "b"}}, p.tags)
}

func TestPoint_Series(t *testing.T) {
	p := NewPoint[int]([]Tag{{"b", "b"}, {"a", "a"}}, time.Unix(100, 0), 100)
	series := p.Series()

	assert.Equal(t, "a=a;b=b", series)

	p = NewPoint[int]([]Tag{}, time.Unix(100, 0), 100)
	series = p.Series()
	assert.Empty(t, series)
}
