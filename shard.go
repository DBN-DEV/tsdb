package memtsdb

import (
	"sync"
	"time"
)

type Shard interface {
	Insert(point Point)
	Query(tag Tag) []Point
}

type shard struct {
	points []Point
	// {key: {value: [offset1, offset2]}}
	index map[string]map[string][]int
	l     sync.RWMutex
}

func NewShard() *shard {
	return &shard{
		index: map[string]map[string][]int{},
	}
}

func (s *shard) Insert(point Point) {
	s.l.Lock()
	defer s.l.Unlock()

	offset := len(s.points)
	s.points = append(s.points, point)

	for _, tag := range point.Tags {
		s.updateIndex(offset, tag)
	}
}

func (s *shard) updateIndex(offset int, tag Tag) {
	if keyM, ok := s.index[tag.Key]; ok {
		if offsets, ok := keyM[tag.Value]; ok {
			keyM[tag.Value] = append(offsets, offset)
			return
		}

		keyM[tag.Value] = []int{offset}
		return
	}

	s.index[tag.Key] = map[string][]int{tag.Value: {offset}}
}

func (s *shard) Query(tag Tag, min, max time.Time) []Point {
	s.l.RLock()
	defer s.l.RUnlock()

	keyM, ok := s.index[tag.Key]
	if !ok {
		return nil
	}

	offsets, ok := keyM[tag.Value]
	if !ok {
		return nil
	}

	var ps []Point
	for _, offset := range offsets {
		p := s.points[offset]
		if p.Time.Before(min) || p.Time.After(max) {
			continue
		}

		ps = append(ps, p)
	}

	return ps
}
