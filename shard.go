package memtsdb

import (
	"sync"
	"time"
)

type Shard interface {
	Insert(point Point)
	Query(tag Tag, min, max time.Time) []int64
	Clear()
}

var _ Shard = (*MemShard)(nil)

type value struct {
	t time.Time
	v int64
}

type MemShard struct {
	values []value

	// {key: {value: [offset1, offset2]}}
	index map[string]map[string][]int
	mu    sync.RWMutex
}

func NewMemShard() *MemShard {
	return &MemShard{
		index: map[string]map[string][]int{},
	}
}

func (s *MemShard) Insert(p Point) {
	s.mu.Lock()
	defer s.mu.Unlock()

	offset := len(s.values)
	s.values = append(s.values, value{t: p.Time, v: p.Field})

	for _, tag := range p.Tags {
		s.updateIndex(offset, tag)
	}
}

func (s *MemShard) updateIndex(offset int, tag Tag) {
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

func (s *MemShard) Query(tag Tag, min, max time.Time) []int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()

	keyM, ok := s.index[tag.Key]
	if !ok {
		return nil
	}

	offsets, ok := keyM[tag.Value]
	if !ok {
		return nil
	}

	var ps []int64
	for _, offset := range offsets {
		p := s.values[offset]
		if p.t.Before(min) || p.t.After(max) {
			continue
		}

		ps = append(ps, p.v)
	}

	return ps
}

func (s *MemShard) Clear() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.values = s.values[:0]
	s.index = make(map[string]map[string][]int)
}
