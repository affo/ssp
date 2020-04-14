package ssp

import (
	"github.com/affo/ssp/values"
)

type TimestampExtractor interface {
	ExtractTime(v values.Value) (ts values.Timestamp, wm values.Timestamp)
}

type TimestampExtractorFn func(v values.Value) (ts values.Timestamp, wm values.Timestamp)

func AssignTimestamp(tse TimestampExtractor) Node {
	return NewNode(func(collector Collector, v values.Value) error {
		ts, wm := tse.ExtractTime(v)
		collector.Collect(values.NewTimestampedValue(ts, wm, v))
		return nil
	})
}
