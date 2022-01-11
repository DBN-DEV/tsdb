package memtsdb

import (
	"sync"
	"time"
)

const _shardGroupDuration = time.Minute

type shardGroup[T any] struct {
	// [min, max)
	max   time.Time
	min   time.Time
	shard *MemShard[T]
}

func newShardGroup[T any](t time.Time, round time.Duration) shardGroup[T] {
	sg := shardGroup[T]{shard: NewMemShard[T]()}
	sg.initTime(t, round)

	return sg
}

func (g *shardGroup[T]) initTime(t time.Time, round time.Duration) {
	rounded := t.Round(round)

	if rounded.Sub(t) >= 0 {
		// round up
		g.min = rounded.Add(-round)
		g.max = rounded
	} else {
		// round down
		g.min = rounded
		g.max = rounded.Add(round)
	}
}

func (g *shardGroup[T]) contains(t time.Time) bool {
	// [min, max)
	if g.min.Before(t) && g.max.After(t) {
		return true
	}

	if g.min.Equal(t) {
		return true
	}

	return false
}

func (g *shardGroup[T]) have(min, max time.Time) bool {
	// [min, max)
	if g.min.Before(min) && g.max.After(min) {
		return true
	}

	if g.min.Before(max) && g.max.After(max) {
		return true
	}

	if g.min.Equal(min) {
		return true
	}

	return false
}

type TSDB[T any] struct {
	rd time.Duration
	mu sync.RWMutex

	stopGC chan struct{}

	sgDuration time.Duration

	sgs      []shardGroup[T]
	emptySgs []shardGroup[T]
}

func NewTSDB[T any](retentionDuration time.Duration) *TSDB[T] {
	t := &TSDB[T]{rd: retentionDuration, stopGC: make(chan struct{}), sgDuration: _shardGroupDuration}

	go t.gcProc()

	return t
}

func (t *TSDB[T]) getShardGroup(ti time.Time) shardGroup[T] {
	for _, sg := range t.sgs {
		if sg.contains(ti) {
			return sg
		}
	}

	var sg shardGroup[T]
	if len(t.emptySgs) == 0 {
		sg = newShardGroup[T](ti, t.sgDuration)
	} else {
		// pop a shardGroup from used groups
		sg = t.emptySgs[len(t.emptySgs)-1]
		t.emptySgs = t.emptySgs[:len(t.emptySgs)-1]
		sg.initTime(ti, t.sgDuration)
	}

	t.sgs = append(t.sgs, sg)

	return sg
}

func (t *TSDB[T]) gc() {
	t.mu.Lock()
	defer t.mu.Unlock()

	now := time.Now()

	var sgs []shardGroup[T]
	for _, sg := range t.sgs {
		if now.Sub(sg.max) > t.rd {
			sg.shard.Clear()
			t.emptySgs = append(t.emptySgs, sg)
		} else {
			sgs = append(sgs, sg)
		}
	}

	t.sgs = sgs
}

func (t *TSDB[T]) gcProc() {
	ticker := time.NewTicker(t.sgDuration)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			t.gc()
		case <-t.stopGC:
			return
		}
	}
}

func (t *TSDB[T]) Stop() {
	t.stopGC <- struct{}{}
}

func (t *TSDB[T]) InsertPoints(points []Point[T]) {
	t.mu.Lock()
	defer t.mu.Unlock()

	for _, point := range points {
		sg := t.getShardGroup(point.Time)
		sg.shard.Insert(point)
	}
}

func (t *TSDB[T]) Query(tag Tag, min, max time.Time) []T {
	t.mu.RLock()
	defer t.mu.RUnlock()

	var ps []T
	for _, sg := range t.sgs {
		if !sg.have(min, max) {
			continue
		}

		sgv := sg.shard.Query(tag, min, max)
		ps = append(ps, sgv...)
	}

	return ps
}
