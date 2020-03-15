package ssp

import "github.com/affo/ssp/values"

type DataStream interface {
	More() bool
	Next() values.Value
}

type sliceStream struct {
	i  int
	vs []values.Value
}

func NewIntValues(ints ...int) []values.Value {
	vs := make([]values.Value, 0, len(ints))
	for _, i := range ints {
		vs = append(vs, values.NewValue(i))
	}
	return vs
}

func NewStreamFromElements(elems ...values.Value) DataStream {
	return &sliceStream{
		vs: elems,
	}
}

func (s *sliceStream) More() bool {
	return s.i < len(s.vs)
}

func (s *sliceStream) Next() values.Value {
	v := s.vs[s.i]
	s.i++
	return v
}

type emptyStream struct{}

func (e emptyStream) More() bool {
	return false
}

func (e emptyStream) Next() values.Value {
	panic("next when no more")
}

func EmptyStream() DataStream {
	return emptyStream{}
}
