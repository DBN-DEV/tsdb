package tsdb

import (
	"errors"
)

var ErrPointMissingTag = errors.New("tsdb: point missing tag")
