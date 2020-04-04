package ssp

import (
	"github.com/affo/ssp/values"
)

var _ Transport = (*infiniteStream)(nil)

type DataStream interface {
	Next() values.Value
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

const defaultBufferSize = 1024

type infiniteStream struct {
	s          chan values.Value
	bufferSize int
	closed     bool
}

func NewInfiniteStream() *infiniteStream {
	is := &infiniteStream{
		bufferSize: defaultBufferSize,
	}
	is.s = make(chan values.Value, is.bufferSize)
	return is
}

func (s *infiniteStream) Collect(v values.Value) {
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
