package ssp

type Node interface {
	Out() Stream
}

type BaseNode struct {
	ID string
}

func (n BaseNode) Out() Stream {
	return NewStream(n)
}

func (n BaseNode) String() string {
	return n.ID
}
