package memtsdb

import (
	"sync"
	"time"
)

const _shardGroupDuration = time.Minute

type shardGroup struct {
	// [min, max)
	max   time.Time
	min   time.Time
	shard Shard
}

func newShardGroup(t time.Time, round time.Duration) shardGroup {
	sg := shardGroup{shard: NewMemShard()}
	sg.initTime(t, round)

	return sg
}

func (g *shardGroup) initTime(t time.Time, round time.Duration) {
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

func (g *shardGroup) contains(t time.Time) bool {
	// [min, max)
	if g.min.Before(t) && g.max.After(t) {
		return true
	}

	if g.min.Equal(t) {
		return true
	}

	return false
}

func (g *shardGroup) have(min, max time.Time) bool {
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

type TSDB struct {
	rd time.Duration
	mu sync.RWMutex

	stopGC chan struct{}

	sgDuration time.Duration

	sgs      []shardGroup
	emptySgs []shardGroup
}

func NewTSDB(retentionDuration time.Duration) *TSDB {
	t := &TSDB{rd: retentionDuration, stopGC: make(chan struct{}), sgDuration: _shardGroupDuration}

	go t.gcProc()

	return t
}

func (t *TSDB) getShardGroup(ti time.Time) shardGroup {
	for _, sg := range t.sgs {
		if sg.contains(ti) {
			return sg
		}
	}

	var sg shardGroup
	if len(t.emptySgs) == 0 {
		sg = newShardGroup(ti, t.sgDuration)
	} else {
		// pop a shardGroup from used groups
		sg = t.emptySgs[len(t.emptySgs)-1]
		t.emptySgs = t.emptySgs[:len(t.emptySgs)-1]
		sg.initTime(ti, t.sgDuration)
	}

	t.sgs = append(t.sgs, sg)

	return sg
}

func (t *TSDB) gc() {
	t.mu.Lock()
	defer t.mu.Unlock()

	now := time.Now()

	var sgs []shardGroup
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

func (t *TSDB) gcProc() {
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

func (t *TSDB) Stop() {
	t.stopGC <- struct{}{}
}

func (t *TSDB) InsertPoints(points []Point) {
	t.mu.Lock()
	defer t.mu.Unlock()

	for _, point := range points {
		sg := t.getShardGroup(point.Time)
		sg.shard.Insert(point)
	}
}

func (t *TSDB) Query(tag Tag, min, max time.Time) []int64 {
	t.mu.RLock()
	defer t.mu.RUnlock()

	var ps []int64
	for _, sg := range t.sgs {
		if !sg.have(min, max) {
			continue
		}

		sgv := sg.shard.Query(tag, min, max)
		ps = append(ps, sgv...)
	}

	return ps
}
