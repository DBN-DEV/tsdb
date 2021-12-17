package memtsdb

import "time"

type Tag struct {
	Key   string
	Value string
}

type Point struct {
	Tags  []Tag
	Time  time.Time
	Field int64
}
