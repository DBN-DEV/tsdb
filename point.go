package tsdb

import (
	"sort"
	"strings"
	"time"
)

const _tagDelimiter = ";"

type Tag struct {
	Key   string
	Value string
}

func (t Tag) String() string {
	var b strings.Builder
	b.Grow(5)

	b.WriteString(t.Key)
	b.WriteString("=")
	b.WriteString(t.Value)

	return b.String()
}

// Point 代表时序数据的一个点，只读的
type Point[T any] struct {
	tags  []Tag
	time  time.Time
	field T
}

// NewPoint New 一个 point ，tag key 不能重复，重复时以后者为准
func NewPoint[T any](tags []Tag, t time.Time, field T) Point[T] {
	ts := make([]Tag, len(tags))
	copy(ts, tags)
	return Point[T]{tags: tags, time: t, field: field}
}

func (p *Point[T]) deduplicateTags() {
	sort.SliceStable(p.tags, func(i, j int) bool { return p.tags[i].Key < p.tags[j].Key })

	var i int
	for j := 1; j < len(p.tags); j++ {
		v := p.tags[j]
		if v.Key != p.tags[i].Key {
			i++
		}
		p.tags[i] = v
	}
	p.tags = p.tags[:i+1]
}

func (p Point[T]) Series() string {
	if len(p.tags) == 0 {
		return ""
	}

	p.deduplicateTags()

	var b strings.Builder
	// 一个 tag 至少三个字符
	b.Grow(len(p.tags) * 3)
	for i, tag := range p.tags {
		if i != 0 {
			b.WriteString(_tagDelimiter)
		}
		b.WriteString(tag.Key)
		b.WriteString("=")
		b.WriteString(tag.Value)
	}

	return b.String()
}
