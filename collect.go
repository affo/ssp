package ssp

import "github.com/affo/ssp/values"

type Collector interface {
	Collect(v values.Value)
}
