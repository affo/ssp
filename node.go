package ssp

type Node interface {
	Out() Stream
	In(ds DataStream)
	Do(collector Collector)
}

type BaseNode struct {
	ID string
}

func (n BaseNode) Out() Stream {
	return NewStream(n)
}

func (n BaseNode) In(ds DataStream) {
	panic("implement me")
}

func (n BaseNode) Do(collector Collector) {
	panic("implement me")
}

func (n BaseNode) String() string {
	return n.ID
}
