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

// Point 代表时序数据的一个点，只读的
type Point[T any] struct {
	tags  []Tag
	time  time.Time
	field T
}

// NewPoint New 一个 point ，tag key 不能重复，重复时以前者为准
func NewPoint[T any](ts []Tag, t time.Time, field T) Point[T] {
	tags := make([]Tag, 0, len(ts))
	key := make(map[string]struct{})
	for _, tag := range ts {
		if _, ok := key[tag.Key]; !ok {
			tags = append(tags, tag)
			key[tag.Key] = struct{}{}
		}
	}

	return Point[T]{tags: tags, time: t, field: field}
}

func (p Point[T]) sortTags() {
	sort.Slice(p.tags, func(i, j int) bool { return p.tags[i].Key < p.tags[j].Key })
}

func (p Point[T]) Series() string {
	if len(p.tags) == 0 {
		return ""
	}

	p.sortTags()

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
