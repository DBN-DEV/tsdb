package memtsdb

type Tag struct {
	Key   string
	Value string
}

type Point struct {
	Tags  []Tag
	Field int64
}
