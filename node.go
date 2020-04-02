package ssp

import (
	"github.com/affo/ssp/values"
)

type Node interface {
	Out() Stream
	Do(collector Collector, v values.Value) error
	Clone() Node
}

type NodeFunc func(state values.Value, collector Collector, v values.Value) (updatedState values.Value, e error)

type AnonymousNode struct {
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
		state0: state0,
		state:  state0,
		do:     do,
	}
}

func (o *AnonymousNode) Out() Stream {
	return NewStream(o)
}

func (o *AnonymousNode) Do(collector Collector, v values.Value) error {
	s, err := o.do(o.state, collector, v)
	if err != nil {
		return err
	}
	o.state = s
	return nil
}

func (o *AnonymousNode) Clone() Node {
	return NewStatefulNode(
		o.state0,
		o.do,
	)
}

func NewLogSink(t values.Type) (Node, *values.List) {
	s := values.NewList(t)
	return NewStatefulNode(s,
		func(state values.Value, collector Collector, v values.Value) (updatedState values.Value, e error) {
			err := state.(*values.List).AddValue(v)
			return state, err
		}), s
}
