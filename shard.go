package tsdb

import (
	"sync"

	"github.com/cespare/xxhash"
)

const _partitionNum = 16

// value 保存时间和值
type value[T any] struct {
	unixNano int64
	v        T
}

// entry 保存 values，目的减少写入已存在系列的数据的锁争用
type entry[T any] struct {
	mu sync.RWMutex

	values []value[T]
}

// newEntry copy value 并构建一个新的 entry
func newEntry[T any](vs []value[T]) *entry[T] {
	values := make([]value[T], 0, len(vs))
	values = append(values, vs...)

	return &entry[T]{values: values}
}

// add 往 entry 中写入数据
func (e *entry[T]) add(values []value[T]) {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.values = append(e.values, values...)
}

// removeBefore 删除小于 unixNano 的数据
func (e *entry[T]) removeBefore(unixNano int64) {
	e.mu.Lock()
	defer e.mu.Unlock()

	values := make([]value[T], 0, len(e.values))
	for _, v := range e.values {
		if v.unixNano >= unixNano {
			values = append(values, v)
		}
	}
	e.values = values
}

// partition hash ring 的一个分片，目的是减少新新系列的锁争用
type partition[T any] struct {
	mu sync.RWMutex
	// 存储系列和值
	// {"series ex:host=A,region=SH":[value1, value2]}
	store map[string]*entry[T]
}

func newPartition[T any]() *partition[T] {
	store := make(map[string]*entry[T])

	return &partition[T]{store: store}
}

// write 往分片中写入数据
func (p *partition[T]) write(key string, values []value[T]) {
	p.mu.RLock()
	e := p.store[key]
	p.mu.RUnlock()
	if e != nil {
		// 大部分情况会走进这个 if 里面，如果 系列 已经存在
		e.add(values)
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	// 因为中间有一段过程没锁，可能有别的协程已经写入，所以再检查一遍
	if e := p.store[key]; e != nil {
		e.add(values)
		return
	}

	e = newEntry(values)
	p.store[key] = e
}

func (p *partition[T]) removeBefore(unixNano int64) {
	p.mu.Lock()
	defer p.mu.Unlock()

	store := make(map[string]*entry[T], len(p.store))
	for k, e := range p.store {
		e.removeBefore(unixNano)
		// cap = 0 说明上次 remove 的时候已经没有 value ， 较大可能后续也没有 value ，就不加入 store 了
		if cap(e.values) != 0 {
			store[k] = e
		}
	}
	p.store = store
}

type shard[T any] struct {
	partitions []*partition[T]
}

func newShard[T any]() *shard[T] {
	partitions := make([]*partition[T], 0, _partitionNum)

	for i := 0; i < _partitionNum; i++ {
		partitions = append(partitions, newPartition[T]())
	}

	return &shard[T]{partitions: partitions}
}

func (s *shard[T]) getPartitions(key string) *partition[T] {
	return s.partitions[int(xxhash.Sum64([]byte(key))%uint64(len(s.partitions)))]
}

func (s *shard[T]) writeMulti(values map[string][]value[T]) {
	for k, v := range values {
		s.getPartitions(k).write(k, v)
	}
}
