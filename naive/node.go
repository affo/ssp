package naive

import "github.com/affo/ssp"

type Node interface {
	ssp.Node
	In(s ssp.DataStream)
	Do() ssp.DataStream
}

type Source struct {
	ds ssp.DataStream
}

func NewSource(ds ssp.DataStream) ssp.Stream {
	s := Source{}
	s.In(ds)
	return s.Out()
}

func (s *Source) In(ds ssp.DataStream) {
	s.ds = ds
}

func (s *Source) Out() ssp.Stream {
	return ssp.NewStream(s)
}

func (s *Source) Do() ssp.DataStream {
	return s.ds
}

func (s *Source) String() string {
	return "naive source"
}

type Mapper struct {
	f  func(v ssp.Value) []ssp.Value
	ds ssp.DataStream
}

func NewMapper(f func(v ssp.Value) []ssp.Value) Node {
	return &Mapper{
		f: f,
	}
}

func (m *Mapper) Out() ssp.Stream {
	return ssp.NewStream(m)
}

func (m *Mapper) In(s ssp.DataStream) {
	m.ds = s
}

func (m *Mapper) Do() ssp.DataStream {
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
	Values []ssp.Value
}

func (s *Sink) Out() ssp.Stream {
	panic("cannot take out stream of sink")
}

func (s *Sink) In(ds ssp.DataStream) {
	for ds.More() {
		s.Values = append(s.Values, ds.Next())
	}
}

func (s *Sink) Do() ssp.DataStream {
	// returned stream should never be used!
	return nil
}

func (s *Sink) String() string {
	return "sink"
}
