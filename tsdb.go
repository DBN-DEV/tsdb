package tsdb

import (
	"time"
)

type TSDB[T any] struct {
	retentionPolicy time.Duration

	s *shard[T]
}

func New[T any](retentionPolicy time.Duration) *TSDB[T] {
	s := newShard[T]()

	return &TSDB[T]{retentionPolicy: retentionPolicy, s: s}
}

func (db *TSDB[T]) WritePoints(points []Point[T]) error {
	values := make(map[string][]value[T], len(points))
	for _, point := range points {
		v := value[T]{unixNano: point.time.UnixNano(), v: point.field}
		s := point.Series()

		if len(s) == 0 {
			return ErrPointMissingTag
		}

		if vs, ok := values[s]; ok {
			values[s] = append(vs, v)
		} else {
			values[s] = []value[T]{v}
		}
	}

	db.s.writeMulti(values)
	return nil
}
