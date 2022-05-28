package tsdb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIndex_createSeriesIfNotExists(t *testing.T) {
	idx := newIndex()

	tags := []Tag{{Key: "A", Value: "B"}, {Key: "C", Value: "D"}}
	idx.createSeriesIfNotExists(map[string][]Tag{"A=B;C=D": tags})
	idx.createSeriesIfNotExists(map[string][]Tag{"A=B;C=D": tags})

	assert.Equal(t, map[string]struct{}{"A=B;C=D": {}}, idx.series)
	assert.Equal(t, []string{"A=B;C=D"}, idx.store["A=B"])
}

func TestIndex_FindSeries(t *testing.T) {
	idx := index{store: map[string][]string{"A=B": {"A=B;C=D"}, "C=D": {"A=B;C=D", "C=D;E=F"}}}
	series := idx.findSeries([]Tag{{Key: "A", Value: "B"}, {Key: "C", Value: "D"}})
	assert.Equal(t, []string{"A=B;C=D"}, series)

	series = idx.findSeries([]Tag{{Key: "X", Value: "Y"}})
	assert.Empty(t, series)
}
