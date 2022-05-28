package tsdb

import "sync"

type index struct {
	mu sync.RWMutex

	// series 快速过滤一个 series 是否存在
	series map[string]struct{}
	// store 存储 tag -> series 的映射关系
	store map[string][]string
}

func newIndex() *index {
	series := make(map[string]struct{})
	store := make(map[string][]string)

	return &index{series: series, store: store}
}

func (i *index) createSeriesIfNotExists(seriesTags map[string][]Tag) {
	i.mu.Lock()
	defer i.mu.Unlock()

	for series, tags := range seriesTags {
		if _, ok := i.series[series]; ok {
			return
		}

		i.series[series] = struct{}{}
		for _, tag := range tags {
			s := tag.String()

			i.store[s] = append(i.store[s], series)
		}
	}
}

func (i *index) findSeries(tags []Tag) []string {
	i.mu.RLock()
	defer i.mu.RUnlock()

	seriesM := make(map[string]struct{})
	for _, tag := range tags {
		ss := i.store[tag.String()]
		for _, s := range ss {
			seriesM[s] = struct{}{}
		}
	}

	series := make([]string, 0, len(seriesM))
	for s := range seriesM {
		series = append(series, s)
	}

	return series
}
