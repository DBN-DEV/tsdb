package memtsdb

import "sync"

type Shard interface {
	Insert(point Point)
	Query(tag Tag) []Point
}

type ShardImpl struct {
	points []Point
	// {key: {value: [offset1, offset2]}}
	index map[string]map[string][]int
	l     sync.RWMutex
}

func NewPool() *ShardImpl {
	return &ShardImpl{
		index: map[string]map[string][]int{},
	}
}

func (s *ShardImpl) Insert(point Point) {
	s.l.Lock()
	defer s.l.Unlock()

	offset := len(s.points)
	s.points = append(s.points, point)

	for _, tag := range point.Tags {
		s.updateIndex(offset, tag)
	}
}

func (s *ShardImpl) updateIndex(offset int, tag Tag) {
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

func (s *ShardImpl) Query(tag Tag) []Point {
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
		ps = append(ps, s.points[offset])
	}

	return ps
}
