package ssp

import (
	"fmt"

	"github.com/affo/ssp/values"
)

type DataStream interface {
	Next() values.Value
	Type() values.Type
}

// TwoWayStream is a DataStream with append capabilities.
type TwoWayStream interface {
	Collector
	DataStream
}

type sliceStream struct {
	i  int
	vs []values.Value
}

func NewIntValues(ints ...int64) []values.Value {
	vs := make([]values.Value, 0, len(ints))
	for _, i := range ints {
		vs = append(vs, values.New(i))
	}
	return vs
}

func NewStreamFromElements(elems ...values.Value) DataStream {
	return &sliceStream{
		vs: elems,
	}
}

func (s *sliceStream) Next() values.Value {
	if s.i >= len(s.vs) {
		return nil
	}
	v := s.vs[s.i]
	s.i++
	return v
}

func (s *sliceStream) Type() values.Type {
	return values.Int64
}

type emptyStream struct {
	t values.Type
}

func NewEmptyStream(t values.Type) *emptyStream {
	return &emptyStream{t: t}
}

func (e emptyStream) Next() values.Value {
	return nil
}

func (e emptyStream) Type() values.Type {
	return e.t
}

type infiniteStream struct {
	s      chan values.Value
	t      values.Type
	closed bool
}

func NewInfiniteStream(t values.Type, bufferSize int) *infiniteStream {
	return &infiniteStream{
		t: t,
		s: make(chan values.Value, bufferSize),
	}
}

func (s *infiniteStream) Collect(v values.Value) {
	if v.Type() != s.t {
		panic(fmt.Errorf("stream of type %v cannot ingest value of type %v", s.t, v.Type()))
	}
	if s.closed {
		panic(fmt.Errorf("attempted to send values to a closed stream"))
	}
	s.s <- v
}

func (s *infiniteStream) Next() values.Value {
	return <-s.s
}

func (s *infiniteStream) Type() values.Type {
	return s.t
}

func (s *infiniteStream) Close() {
	s.closed = true
	close(s.s)
}
