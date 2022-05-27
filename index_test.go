package tsdb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIndex_createSeriesIfNotExists(t *testing.T) {
	idx := newIndex()

	tags := []Tag{{"A", "B"}, {"C", "D"}}
	idx.createSeriesIfNotExists(map[string][]Tag{"A=B;C=D": tags})
	idx.createSeriesIfNotExists(map[string][]Tag{"A=B;C=D": tags})

	assert.Equal(t, map[string]struct{}{"A=B;C=D": {}}, idx.series)
	assert.Equal(t, []string{"A=B;C=D"}, idx.store["A=B"])
}
