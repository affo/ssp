package ssp

type DataStream interface {
	More() bool
	Next() Value
}

type sliceStream struct {
	i  int
	vs []Value
}

func NewIntValues(ints ...int) []Value {
	vs := make([]Value, 0, len(ints))
	for _, i := range ints {
		vs = append(vs, NewValue(i))
	}
	return vs
}

func NewStreamFromElements(elems ...Value) DataStream {
	return &sliceStream{
		vs: elems,
	}
}

func (s *sliceStream) More() bool {
	return s.i < len(s.vs)
}

func (s *sliceStream) Next() Value {
	v := s.vs[s.i]
	s.i++
	return v
}

type emptyStream struct{}

func (e emptyStream) More() bool {
	return false
}

func (e emptyStream) Next() Value {
	panic("next when no more")
}

func EmptyStream() DataStream {
	return emptyStream{}
}
