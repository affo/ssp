package ssp

import (
	"fmt"

	"github.com/affo/ssp/values"
)

var _ Transport = (*infiniteStream)(nil)

type DataStream interface {
	Next() values.Value
	Type() values.Type
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

type infiniteStream struct {
	t      values.Type
	s      chan values.Value
	closed bool
	opts   []StreamOption

	steer      Steer
	bufferSize int
}

type StreamOption func(*infiniteStream)

func WithSteer(steer Steer) StreamOption {
	return func(s *infiniteStream) {
		s.steer = steer
	}
}

func WithBuffer(size int) StreamOption {
	return func(s *infiniteStream) {
		s.bufferSize = size
	}
}

func NewInfiniteStream(t values.Type, opts ...StreamOption) *infiniteStream {
	is := &infiniteStream{
		t:    t,
		opts: opts,
	}
	for _, opt := range opts {
		opt(is)
	}
	is.s = make(chan values.Value, is.bufferSize)
	return is
}

func (s *infiniteStream) Collect(v values.Value) {
	if s.closed {
		// The stream has already been closed. Do not collect.
		return
	}
	if v.Type() != values.Close && v.Type() != s.t {
		panic(fmt.Errorf("stream of type %v cannot ingest value of type %v", s.t, v.Type()))
	}
	if s.steer != nil {
		k := s.steer.Assign(v)
		v = values.NewKeyedValue(k, v)
	}
	s.s <- v
}

func (s *infiniteStream) close() {
	s.closed = true
	close(s.s)
}

func (s *infiniteStream) Next() values.Value {
	v := <-s.s
	if v.Type() == values.Close {
		s.close()
		return nil
	}
	return v
}

func (s *infiniteStream) Type() values.Type {
	return s.t
}

func (s *infiniteStream) Clone() Transport {
	return NewInfiniteStream(s.t, s.opts...)
}
