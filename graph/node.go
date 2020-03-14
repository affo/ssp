package graph

type BaseNode struct {
	ID string
}

func (n BaseNode) Out() Arch {
	return NewArch(n)
}

func (n BaseNode) String() string {
	return n.ID
}
