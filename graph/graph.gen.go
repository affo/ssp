// Generated by tmpl
// https://github.com/benbjohnson/tmpl
//
// DO NOT EDIT!
// Source: graph.go.tmpl

package graph

import (
	"context"
	"fmt"
	"sort"
	"strings"
)

type graphContextKey int

const key graphContextKey = iota

type Graph struct {
	adjacency map[Node][]Arch
	roots     map[Node]bool
}

func (g *Graph) add(a Arch) {
	g.adjacency[a.From()] = append(g.adjacency[a.From()], a)
	if _, ok := g.roots[a.From()]; !ok {
		g.roots[a.From()] = true
	}
	g.roots[a.To()] = false
}

func (g Graph) Roots() (roots []Node) {
	for n, root := range g.roots {
		if root {
			roots = append(roots, n)
		}
	}
	sort.Sort(nodesByRepr(roots))
	return roots
}

func (g Graph) Adjacents(n Node) []Arch {
	return g.adjacency[n]
}

// TODO(affo): more walking strategy and complete visitor class.
type Visitor func(a Arch)

type nodesByRepr []Node

func nodeToString(n Node) string {
	return fmt.Sprintf("%v", n)
}

func (n nodesByRepr) Len() int {
	return len(n)
}

func (n nodesByRepr) Less(i, j int) bool {
	return strings.Compare(nodeToString(n[i]), nodeToString(n[j])) < 0
}

func (n nodesByRepr) Swap(i, j int) {
	n[i], n[j] = n[j], n[i]
}

func (g Graph) Walk(v Visitor) {
	// Walk in predictable order.
	keys := make(nodesByRepr, 0, len(g.adjacency))
	for n := range g.adjacency {
		keys = append(keys, n)
	}
	sort.Sort(keys)
	for _, from := range keys {
		adj := g.adjacency[from]
		for _, a := range adj {
			v(a)
		}
	}
}

func (g Graph) String() string {
	sb := strings.Builder{}
	g.Walk(func(a Arch) {
		sb.WriteString(fmt.Sprintf("%v", a))
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
	ctx = setGraph(ctx, Graph{
		adjacency: make(map[Node][]Arch),
		roots:     make(map[Node]bool),
	})
	return ctx
}

type Node interface {
	Out() Arch
}

type Arch interface {
	From() Node
	To() Node
	Connect(ctx context.Context, node Node, name string) Node
	Name() string
}

type arch struct {
	from Node
	to   Node
	name string
}

func NewArch(from Node) Arch {
	return &arch{
		from: from,
	}
}

func (a *arch) From() Node {
	return a.from
}

func (a *arch) To() Node {
	return a.to
}

func (a *arch) Connect(ctx context.Context, node Node, name string) Node {
	g := getGraph(ctx)
	clone := &arch{
		from: a.from,
		to:   node,
		name: name,
	}
	g.add(clone)
	return node
}

func (a *arch) String() string {
	sb := strings.Builder{}
	sb.WriteString(fmt.Sprintf("%v -> %v [", a.from, a.to))
	sb.WriteString(fmt.Sprintf("name: %v", a.name))
	sb.WriteRune(']')
	return sb.String()
}

func (a *arch) Name() string {
	return a.name
}
