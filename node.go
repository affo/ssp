package ssp

import (
	"github.com/affo/ssp/values"
)

type Node interface {
	Do(collector Collector, v values.Value) error
	Out() *Arch
	Clone() Node

	// Options.
	SetParallelism(par int) Node
	GetParallelism() int
	SetName(name string) Node
	GetName() string
}

type baseNode struct {
	par  int
	name string
}

func newBaseNode() baseNode {
	return baseNode{par: 1}
}

func (n baseNode) GetParallelism() int {
	return n.par
}

func (n baseNode) GetName() string {
	return n.name
}

func (n baseNode) String() string {
	return n.name
}

func (n baseNode) Clone() baseNode {
	return baseNode{
		par:  n.par,
		name: n.name,
	}
}

type NodeFunc func(state values.Value, collector Collector, v values.Value) (values.Value, error)

type AnonymousNode struct {
	baseNode

	state0 values.Value
	state  values.Value
	do     NodeFunc
}

func NewNode(do func(collector Collector, v values.Value) error) *AnonymousNode {
	return NewStatefulNode(
		values.NewNull(values.Int64),
		func(state values.Value, collector Collector, v values.Value) (value values.Value, e error) {
			return state, do(collector, v)
		},
	)
}

func NewStatefulNode(state0 values.Value, do NodeFunc) *AnonymousNode {
	return &AnonymousNode{
		baseNode: newBaseNode(),
		state0:   state0,
		state:    state0,
		do:       do,
	}
}

func (n *AnonymousNode) Do(collector Collector, v values.Value) error {
	s, err := n.do(n.state, collector, v)
	if err != nil {
		return err
	}
	n.state = s
	return nil
}

func (n *AnonymousNode) Out() *Arch {
	return NewLink(n)
}

func (n *AnonymousNode) SetParallelism(par int) Node {
	n.baseNode.par = par
	return n
}

func (n *AnonymousNode) SetName(name string) Node {
	n.baseNode.name = name
	return n
}

func (n *AnonymousNode) Clone() Node {
	return &AnonymousNode{
		baseNode: n.baseNode.Clone(),
		state0:   n.state0,
		state:    n.state0,
		do:       n.do,
	}
}

func NewLogSink(t values.Type) (Node, *values.List) {
	s := values.NewList(t)
	return NewStatefulNode(s,
		func(state values.Value, collector Collector, v values.Value) (values.Value, error) {
			err := state.(*values.List).AddValue(v)
			return state, err
		}), s
}
