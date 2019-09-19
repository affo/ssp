package graph

import (
	"context"
	"fmt"
	"strings"
)

type graphContextKey int

const key graphContextKey = iota

type Graph struct {
	arches []Arch
}

// TODO(affo): we'll do more about this
type Visitor func(a Arch)

func (g Graph) Walk(v Visitor) {
	for _, a := range g.arches {
		v(a)
	}
}

func (g Graph) String() string {
	sb := strings.Builder{}
	g.Walk(func(a Arch) {
		sb.WriteString(a.String())
		sb.WriteRune('\n')
	})
	return sb.String()
}

func getGraph(ctx context.Context) *Graph {
	return ctx.Value(key).(*Graph)
}

func GetGraph(ctx context.Context) Graph {
	return *ctx.Value(key).(*Graph)
}

func setGraph(ctx context.Context, g Graph) context.Context {
	return context.WithValue(ctx, key, &g)
}

func Context() context.Context {
	ctx := context.Background()
	ctx = setGraph(ctx, Graph{arches: []Arch{}})
	return ctx
}

type Node interface {
	Out() Arch
	String() string
}

type Arch interface {
	From() Node
	To() Node
	Connect(ctx context.Context, node Node) Node
	String() string
}

type arch struct {
	from Node
	to   Node
}

func NewArch(from Node) Arch {
	return &arch{from: from}
}

func (a *arch) From() Node {
	return a.from
}

func (a *arch) To() Node {
	return a.to
}

func (a *arch) Connect(ctx context.Context, node Node) Node {
	g := getGraph(ctx)
	clone := &arch{
		from: a.from,
		to:   node,
	}
	g.arches = append(g.arches, clone)
	return node
}

func (a *arch) String() string {
	return fmt.Sprintf("%v -> %v", a.from, a.to)
}
