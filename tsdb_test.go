package memtsdb

import (
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

func TestShardGroup_contains(t *testing.T) {
	g := shardGroup{min: time.Unix(2, 0), max: time.Unix(10, 0)}

	assert.True(t, g.contains(time.Unix(5, 0)))
	assert.True(t, g.contains(time.Unix(2, 0)))

	assert.False(t, g.contains(time.Unix(10, 0)))

	assert.False(t, g.contains(time.Unix(1, 0)))
	assert.False(t, g.contains(time.Unix(11, 0)))
}

func TestShardGroup_init(t *testing.T) {
	round := 1 * time.Minute
	g := shardGroup{}

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
	g := shardGroup{min: time.Unix(5, 0), max: time.Unix(10, 0)}

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
	_ = NewTSDB(1 * time.Minute)
}

func TestTSDB_getShardGroup(t *testing.T) {
	{
		// exited SG
		sg := shardGroup{min: time.Unix(1, 0), max: time.Unix(5, 0)}
		db := TSDB{sgs: []shardGroup{sg}}
		r := db.getShardGroup(time.Unix(4, 0))
		assert.Equal(t, sg, r)
	}
	{
		// empty sg
		db := TSDB{}
		r := db.getShardGroup(time.Unix(5, 0))
		assert.Equal(t, time.Unix(0, 0), r.min)
		assert.Equal(t, time.Unix(60, 0), r.max)
		assert.Len(t, db.sgs, 1)
	}
	{
		// reuse sg
		sg := shardGroup{min: time.Unix(1, 0), max: time.Unix(5, 0)}
		db := TSDB{emptySgs: []shardGroup{sg}}
		r := db.getShardGroup(time.Unix(4, 0))
		assert.Equal(t, time.Unix(0, 0), r.min)
		assert.Equal(t, time.Unix(60, 0), r.max)
	}
}

func TestTSDB_InsertPoints(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ps := []Point{{
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
	s := NewMockShard(ctrl)
	s.EXPECT().Insert(ps[0]).Times(1)
	s.EXPECT().Insert(ps[1]).Times(1)

	db := TSDB{sgs: []shardGroup{{min: time.Unix(0, 0), max: time.Unix(60, 0), shard: s}}}
	db.InsertPoints(ps)
}

func TestTSDB_Query(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ps := []Point{{
		Tags:        []Tag{{Key: "a", Value: "b"}},
		Measurement: "mea",
		Time:        time.Unix(1, 0),
		Field:       100,
	}, {
		Tags:        []Tag{{Key: "a", Value: "b"}},
		Measurement: "mea",
		Time:        time.Unix(30, 0),
		Field:       200,
	}}
	s := NewMockShard(ctrl)
	s.EXPECT().Query(Tag{Key: "a", Value: "b"}, time.Unix(0, 0), time.Unix(30, 0)).Return(ps).Times(1)

	sgs := []shardGroup{{
		min:   time.Unix(0, 0),
		max:   time.Unix(60, 0),
		shard: s,
	}, {
		min:   time.Unix(60, 0),
		max:   time.Unix(120, 0),
		shard: s,
	}}

	db := TSDB{sgs: sgs}
	r := db.Query(Tag{Key: "a", Value: "b"}, time.Unix(0, 0), time.Unix(30, 0))
	assert.Equal(t, ps, r)
}
