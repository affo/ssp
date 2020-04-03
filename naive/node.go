package naive

type Node interface {
	Out() Link
	In(s DataStream)
	Do() DataStream
}

type Source struct {
	ds DataStream
}

func NewSource(ds DataStream) Link {
	s := Source{}
	s.In(ds)
	return s.Out()
}

func (s *Source) In(ds DataStream) {
	s.ds = ds
}

func (s *Source) Out() Link {
	return NewLink(s)
}

func (s *Source) Do() DataStream {
	return s.ds
}

func (s *Source) String() string {
	return "naive source"
}

type Mapper struct {
	f  func(v int) []int
	ds DataStream
}

func NewMapper(f func(v int) []int) Node {
	return &Mapper{
		f: f,
	}
}

func (m *Mapper) Out() Link {
	return NewLink(m)
}

func (m *Mapper) In(s DataStream) {
	m.ds = s
}

func (m *Mapper) Do() DataStream {
	sb := NewStreamBuilder()
	for m.ds.More() {
		vs := m.f(m.ds.Next())
		for _, v := range vs {
			sb.Add(v)
		}
	}
	return sb.Stream()
}

func (m *Mapper) String() string {
	return "mapper"
}

type Sink struct {
	Values []int
}

func (s *Sink) Out() Link {
	panic("cannot take out stream of sink")
}

func (s *Sink) In(ds DataStream) {
	for ds.More() {
		s.Values = append(s.Values, ds.Next())
	}
}

func (s *Sink) Do() DataStream {
	// returned stream should never be used!
	return nil
}

func (s *Sink) String() string {
	return "sink"
}
