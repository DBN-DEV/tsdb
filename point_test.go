package tsdb

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestTag_String(t *testing.T) {
	tag := Tag{Key: "a", Value: "b"}
	s := tag.String()

	assert.Equal(t, "a=b", s)
}

func TestPoint_DeduplicateTags(t *testing.T) {
	p := NewPoint[int]([]Tag{{"b", "b"}, {"a", "a"}, {"a", "c"}}, time.Unix(100, 0), 100)
	p.deduplicateTags()

	assert.Equal(t, []Tag{{"a", "c"}, {"b", "b"}}, p.tags)
}

func TestPoint_Series(t *testing.T) {
	p := NewPoint[int]([]Tag{{"b", "b"}, {"a", "a"}}, time.Unix(100, 0), 100)
	series := p.Series()

	assert.Equal(t, "a=a;b=b", series)

	p = NewPoint[int]([]Tag{}, time.Unix(100, 0), 100)
	series = p.Series()
	assert.Empty(t, series)
}
