package graph

type Node interface {
	Out() Link
}

type BaseNode struct {
	ID string
}

func (n BaseNode) Out() Link {
	return NewLink(n)
}

func (n BaseNode) String() string {
	return n.ID
}
