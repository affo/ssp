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

	setTime(ts, wm Timestamp)
}

var _ TimestampedValue = (*timestampedValue)(nil)

type timestampedValue struct {
	ts Timestamp
	wm Timestamp
	Value
}

func (v *timestampedValue) Timestamp() Timestamp {
	return v.ts
}

func (v *timestampedValue) Watermark() Timestamp {
	return v.wm
}

func (v *timestampedValue) setTime(ts, wm Timestamp) {
	v.ts = ts
	v.wm = wm
}

func (v *timestampedValue) Unwrap() (Value, error) {
	return v.Value, nil
}

func (v *timestampedValue) Clone() Value {
	c := v.Value.Clone()
	return SetTime(v.ts, v.wm, c)
}

func SetTime(ts, wm Timestamp, v Value) Value {
	tsv, err := GetTimestampedValue(v)
	if err != nil {
		return &timestampedValue{ts: ts, wm: wm, Value: v}
	}
	tsv.setTime(ts, wm)
	return v
}

func GetTimestampedValue(v Value) (TimestampedValue, error) {
	for {
		if tsv, ok := v.(TimestampedValue); ok {
			return tsv, nil
		}
		uv, err := v.Unwrap()
		if err != nil {
			return nil, err
		}
		v = uv
	}
}

func GetTime(v Value) (Timestamp, Timestamp, error) {
	tsv, err := GetTimestampedValue(v)
	if err != nil {
		return 0, 0, err
	}
	return tsv.Timestamp(), tsv.Watermark(), nil
}
