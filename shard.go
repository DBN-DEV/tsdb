package memtsdb

import (
	"sync"
	"time"
)

type value[T any] struct {
	t time.Time
	v T
}

type MemShard[T any] struct {
	values []value[T]

	// {key: {value: [offset1, offset2]}}
	index map[string]map[string][]int
	mu    sync.RWMutex
}

func NewMemShard[T any]() *MemShard[T] {
	return &MemShard[T]{
		index: map[string]map[string][]int{},
	}
}

func (s *MemShard[T]) Insert(p Point[T]) {
	s.mu.Lock()
	defer s.mu.Unlock()

	offset := len(s.values)
	s.values = append(s.values, value[T]{t: p.Time, v: p.Field})

	for _, tag := range p.Tags {
		s.updateIndex(offset, tag)
	}
}

func (s *MemShard[T]) updateIndex(offset int, tag Tag) {
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

func (s *MemShard[T]) Query(tag Tag, min, max time.Time) []T {
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

	var ps []T
	for _, offset := range offsets {
		p := s.values[offset]
		if p.t.Before(min) || p.t.After(max) {
			continue
		}

		ps = append(ps, p.v)
	}

	return ps
}

func (s *MemShard[T]) Clear() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.values = s.values[:0]
	s.index = make(map[string]map[string][]int)
}
