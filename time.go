package ssp

import (
	"github.com/affo/ssp/values"
)

type TimestampExtractor interface {
	ExtractTime(v values.Value) (ts values.Timestamp, wm values.Timestamp)
}

type TimestampExtractorFn func(v values.Value) (ts values.Timestamp, wm values.Timestamp)

func AssignTimestamp(tse TimestampExtractorFn) Node {
	return NewNode(func(collector Collector, v values.Value) error {
		ts, wm := tse(v)
		collector.Collect(values.SetTime(ts, wm, v))
		return nil
	})
}
