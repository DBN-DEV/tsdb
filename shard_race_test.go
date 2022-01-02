package memtsdb

import (
	"sync"
	"testing"
	"time"
)

func TestMemShardRace(t *testing.T) {
	s := NewMemShard()

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		for i := 0; i < 1000; i++ {
			ti := time.Unix(int64(i), 0)
			p := Point{Tags: []Tag{{Key: "a", Value: "b"}}, Time: ti, Field: 100}
			s.Insert(p)
		}
		wg.Done()
	}()

	wg.Add(1)
	go func() {
		for i := 0; i < 1000; i++ {
			ti := time.Unix(int64(i), 0)
			s.Query(Tag{Key: "a", Value: "b"}, ti, ti)
		}
		wg.Done()
	}()

	wg.Add(1)
	go func() {
		for i := 0; i < 1000; i++ {
			s.Clear()
		}
		wg.Done()
	}()

	wg.Wait()
}
