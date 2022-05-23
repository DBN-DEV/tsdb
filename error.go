package tsdb

import (
	"errors"
)

var (
	ErrPointMissingTag = errors.New("tsdb: point missing tag")
	ErrDBClosed        = errors.New("tsdb: db is closed")
)
