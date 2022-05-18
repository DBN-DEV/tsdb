package tsdb

import (
	"context"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestEntry_Add_Race(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup

	var e entry[int]
	var total int64
	num := 24
	for i := 0; i < num; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
					e.add([]value[int]{{100, 200}})
					atomic.AddInt64(&total, 1)
					time.Sleep(100 * time.Microsecond)
				}
			}
		}()
	}

	time.Sleep(time.Second)

	cancel()
	wg.Wait()

	assert.Len(t, e.values, int(total))
}

func TestPartition_Write_Race(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup

	p := partition[int]{store: make(map[string]*entry[int])}
	series := []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k"}
	seriesTotal := make(map[string]*int64)
	for _, s := range series {
		var total int64
		seriesTotal[s] = &total
	}
	num := 10
	for i := 0; i < num; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
					key := series[rand.Intn(len(series))]
					p.write(key, []value[int]{{100, 200}})
					total := seriesTotal[key]
					atomic.AddInt64(total, 1)
					time.Sleep(100 * time.Microsecond)
				}
			}
		}()
	}

	time.Sleep(time.Second)

	cancel()
	wg.Wait()

	for s, e := range p.store {
		total := seriesTotal[s]
		assert.Len(t, e.values, int(*total))
	}
}

func TestShard_WriteMulti_Race(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup

	s := newShard[int](0, 0)
	series := []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k"}
	seriesTotal := make(map[string]*int64)
	for _, s := range series {
		var total int64
		seriesTotal[s] = &total
	}
	num := 10
	for i := 0; i < num; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
					key := series[rand.Intn(len(series))]
					s.writeMulti(map[string][]value[int]{key: {{100, 200}}})
					total := seriesTotal[key]
					atomic.AddInt64(total, 1)
					time.Sleep(100 * time.Microsecond)
				}
			}
		}()
	}

	time.Sleep(time.Second)

	cancel()
	wg.Wait()

	for key, total := range seriesTotal {
		p := s.getPartitions(key)
		assert.Len(t, p.store[key].values, int(*total))
	}
}
