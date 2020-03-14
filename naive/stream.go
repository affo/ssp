package naive

import "github.com/affo/ssp"

type sliceStream struct {
	i  int
	vs []ssp.Value
}

func NewIntValues(ints ...int) []ssp.Value {
	vs := make([]ssp.Value, 0, len(ints))
	for _, i := range ints {
		vs = append(vs, ssp.NewValue(i))
	}
	return vs
}

func NewStreamFromElements(elems ...ssp.Value) ssp.DataStream {
	return &sliceStream{
		vs: elems,
	}
}

func (s *sliceStream) More() bool {
	return s.i < len(s.vs)
}

func (s *sliceStream) Next() ssp.Value {
	v := s.vs[s.i]
	s.i++
	return v
}

type emptyStream struct{}

func (e emptyStream) More() bool {
	return false
}

func (e emptyStream) Next() ssp.Value {
	panic("next when no more")
}

func EmptyStream() ssp.DataStream {
	return emptyStream{}
}

type StreamBuilder interface {
	Add(v ssp.Value)
	Stream() ssp.DataStream
}

type sliceStreamBuilder struct {
	ss sliceStream
}

func NewStreamBuilder() StreamBuilder {
	return &sliceStreamBuilder{ss: sliceStream{}}
}

func (s *sliceStreamBuilder) Add(v ssp.Value) {
	s.ss.vs = append(s.ss.vs, v)
}

func (s *sliceStreamBuilder) Stream() ssp.DataStream {
	return &s.ss
}
