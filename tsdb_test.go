package memtsdb

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestShardGroup_contains(t *testing.T) {
	g := shardGroup[int]{min: time.Unix(2, 0), max: time.Unix(10, 0)}

	assert.True(t, g.contains(time.Unix(5, 0)))
	assert.True(t, g.contains(time.Unix(2, 0)))

	assert.False(t, g.contains(time.Unix(10, 0)))

	assert.False(t, g.contains(time.Unix(1, 0)))
	assert.False(t, g.contains(time.Unix(11, 0)))
}

func TestShardGroup_init(t *testing.T) {
	round := 1 * time.Minute
	g := shardGroup[int]{}

	// round up
	g.initTime(time.Unix(1, 0), round)
	assert.Equal(t, time.Unix(60, 0), g.max)
	assert.Equal(t, time.Unix(0, 0), g.min)

	// round down
	g.initTime(time.Unix(31, 0), round)
	assert.Equal(t, time.Unix(60, 0), g.max)
	assert.Equal(t, time.Unix(0, 0), g.min)
}

func TestShardGroup_have(t *testing.T) {
	g := shardGroup[int]{min: time.Unix(5, 0), max: time.Unix(10, 0)}

	assert.True(t, g.have(time.Unix(4, 0), time.Unix(6, 0)))
	assert.True(t, g.have(time.Unix(6, 0), time.Unix(11, 0)))
	assert.True(t, g.have(time.Unix(6, 0), time.Unix(9, 0)))

	assert.True(t, g.have(time.Unix(5, 0), time.Unix(11, 0)))
	assert.True(t, g.have(time.Unix(6, 0), time.Unix(10, 0)))

	assert.False(t, g.have(time.Unix(3, 0), time.Unix(4, 0)))
	assert.False(t, g.have(time.Unix(11, 0), time.Unix(12, 0)))
	assert.False(t, g.have(time.Unix(10, 0), time.Unix(11, 0)))
}

func TestNewTSDB(t *testing.T) {
	tsdb := NewTSDB[int](1 * time.Minute)
	tsdb.Stop()
}

func TestTSDB_getShardGroup(t *testing.T) {
	{
		// exited SG
		sg := shardGroup[int]{min: time.Unix(1, 0), max: time.Unix(5, 0)}
		db := TSDB[int]{sgs: []shardGroup[int]{sg}}
		r := db.getShardGroup(time.Unix(4, 0))
		assert.Equal(t, sg, r)
	}
	{
		// empty sg
		db := TSDB[int]{sgDuration: time.Minute}
		r := db.getShardGroup(time.Unix(5, 0))
		assert.Equal(t, time.Unix(0, 0), r.min)
		assert.Equal(t, time.Unix(60, 0), r.max)
		assert.Len(t, db.sgs, 1)
	}
	{
		// reuse sg
		sg := shardGroup[int]{min: time.Unix(1, 0), max: time.Unix(5, 0)}
		db := TSDB[int]{emptySgs: []shardGroup[int]{sg}, sgDuration: time.Minute}
		r := db.getShardGroup(time.Unix(4, 0))
		assert.Equal(t, time.Unix(0, 0), r.min)
		assert.Equal(t, time.Unix(60, 0), r.max)
	}
}

func TestTSDB_InsertPoints(t *testing.T) {
	ps := []Point[int]{{
		Tags:        []Tag{{Key: "a", Value: "b"}},
		Measurement: "mea",
		Time:        time.Unix(1, 0),
		Field:       100,
	}, {
		Tags:        []Tag{{Key: "a", Value: "b"}},
		Measurement: "mea",
		Time:        time.Unix(2, 0),
		Field:       200,
	}}

	shard := NewMemShard[int]()
	db := TSDB[int]{sgs: []shardGroup[int]{{min: time.Unix(0, 0), max: time.Unix(60, 0), shard: shard}}}
	db.InsertPoints(ps)
}

func TestTSDB_Query(t *testing.T) {
	s := NewMemShard[int]()
	emptyS := NewMemShard[int]()

	s.Insert(Point[int]{
		Measurement: "a",
		Tags:        []Tag{{Key: "a", Value: "b"}},
		Time:        time.Unix(20, 0),
		Field:       100,
	})
	s.Insert(Point[int]{
		Measurement: "a",
		Tags:        []Tag{{Key: "a", Value: "b"}},
		Time:        time.Unix(21, 0),
		Field:       200,
	})

	sgs := []shardGroup[int]{{
		min:   time.Unix(0, 0),
		max:   time.Unix(60, 0),
		shard: s,
	}, {
		min:   time.Unix(60, 0),
		max:   time.Unix(120, 0),
		shard: emptyS,
	}}

	db := TSDB[int]{sgs: sgs}
	r := db.Query(Tag{Key: "a", Value: "b"}, time.Unix(0, 0), time.Unix(30, 0))
	assert.Equal(t, []int{100, 200}, r)
}

func TestTSDB_GC(t *testing.T) {
	expiredS := NewMemShard[int]()
	expiredSg := shardGroup[int]{max: time.Unix(100, 0), shard: expiredS}

	s := NewMemShard[int]()
	sg := shardGroup[int]{max: time.Now().Add(time.Hour), shard: s}
	tsdb := TSDB[int]{rd: time.Minute, sgs: []shardGroup[int]{expiredSg, sg}}
	tsdb.gc()

	assert.Len(t, tsdb.sgs, 1)
	assert.Len(t, tsdb.emptySgs, 1)

	assert.Equal(t, expiredSg, tsdb.emptySgs[0])
	assert.Equal(t, &sg, &tsdb.sgs[0])
}

func TestTSDB_Stop(t *testing.T) {
	ch := make(chan struct{}, 1)

	tsdb := TSDB[int]{stopGC: ch}
	tsdb.Stop()

	assert.Eventually(t, func() bool {
		<-ch
		return true
	}, time.Second, 100*time.Microsecond)
}

func TestTSDB_GCProc(t *testing.T) {
	expiredS := NewMemShard[int]()
	expiredSg := shardGroup[int]{max: time.Unix(100, 0), shard: expiredS}

	tsdb := TSDB[int]{
		rd:         time.Minute,
		sgs:        []shardGroup[int]{expiredSg},
		sgDuration: time.Microsecond,
		stopGC:     make(chan struct{}),
	}
	go tsdb.gcProc()
	time.Sleep(500 * time.Microsecond)
	tsdb.Stop()

	assert.Empty(t, tsdb.sgs)
	assert.Len(t, tsdb.emptySgs, 1)
}
