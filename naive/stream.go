package naive

type DataStream interface {
	More() bool
	Next() int
}

type sliceStream struct {
	i  int
	vs []int
}

func NewStreamFromElements(elems ...int) DataStream {
	return &sliceStream{
		vs: elems,
	}
}

func (s *sliceStream) More() bool {
	return s.i < len(s.vs)
}

func (s *sliceStream) Next() int {
	v := s.vs[s.i]
	s.i++
	return v
}

type StreamBuilder interface {
	Add(v int)
	Stream() DataStream
}

type sliceStreamBuilder struct {
	ss sliceStream
}

func NewStreamBuilder() StreamBuilder {
	return &sliceStreamBuilder{ss: sliceStream{}}
}

func (s *sliceStreamBuilder) Add(v int) {
	s.ss.vs = append(s.ss.vs, v)
}

func (s *sliceStreamBuilder) Stream() DataStream {
	return &s.ss
}
