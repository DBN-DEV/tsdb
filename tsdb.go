package tsdb

import (
	"time"
)

type TSDB[T any] struct {
	retentionPolicy time.Duration

	stop     chan struct{}
	isClosed bool

	s *shard[T]
}

func New[T any](retentionPolicy time.Duration) *TSDB[T] {
	s := newShard[T]()
	stop := make(chan struct{})
	db := &TSDB[T]{retentionPolicy: retentionPolicy, s: s, stop: stop}

	go db.gc()

	return db
}

func (db *TSDB[T]) WritePoints(points []Point[T]) error {
	if db.isClosed {
		return ErrDBClosed
	}

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

func (db *TSDB[T]) Stop() {
	db.stop <- struct{}{}
	db.isClosed = true
}

func (db *TSDB[T]) gc() {
	ticker := time.NewTicker(db.retentionPolicy)
	defer ticker.Stop()

	for {
		select {
		case <-db.stop:
			return
		case <-ticker.C:
			remove := time.Now().Add(-db.retentionPolicy).UnixNano()
			db.s.removeBefore(remove)
		}
	}
}
