package ssp

import (
	"github.com/affo/ssp/values"
)

type Collector interface {
	Collect(v values.Value)
}

func SendClose(c Collector) {
	c.Collect(values.NewMeta(values.Close))
}

// Transport enables collecting on a DataStream.
type Transport interface {
	Collector
	DataStream
	Clone() Transport
}
