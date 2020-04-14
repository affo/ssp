package values

import (
	"time"
)

type Timestamp int64

func ConvertTimestamp(ts Timestamp) time.Time {
	return time.Unix(0, int64(ts))
}

func ConvertTime(ts time.Time) Timestamp {
	return Timestamp(ts.UnixNano())
}

func (ts Timestamp) String() string {
	return ConvertTimestamp(ts).Format(time.RFC3339Nano)
}

type TimestampedValue interface {
	Value
	Timestamp() Timestamp
	Watermark() Timestamp
}

type timestampedValue struct {
	ts Timestamp
	wm Timestamp
	Value
}

func NewTimestampedValue(ts, wm Timestamp, v Value) TimestampedValue {
	return timestampedValue{ts: ts, wm: wm, Value: v}
}

func (v timestampedValue) Timestamp() Timestamp {
	return v.ts
}

func (v timestampedValue) Watermark() Timestamp {
	return v.wm
}

func (v timestampedValue) Unwrap() Value {
	return v.Value
}

func GetTime(v Value) (Timestamp, Timestamp) {
	for {
		if tsv, ok := v.(TimestampedValue); ok {
			return tsv.Timestamp(), tsv.Watermark()
		}
		v = v.Unwrap()
	}
}

func SetWatermark(tsv TimestampedValue, newWm Timestamp) TimestampedValue {
	uv := tsv.Unwrap()
	return NewTimestampedValue(tsv.Timestamp(), newWm, uv)
}
