package tsdb

import "time"

type Tag struct {
	Key   string
	Value string
}

type Point[T any] struct {
	Measurement string
	Tags        []Tag
	Time        time.Time
	Field       T
}
