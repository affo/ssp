package ssp

import (
	"fmt"

	"github.com/affo/ssp/values"
)

type Node interface {
	Out() Stream
	Do(collector Collector, v values.Value) error
	InTypes() []values.Type
	OutType() values.Type
	Clone() Node
}

type NodeFunc func(state values.Value, collector Collector, v values.Value) (updatedState values.Value, e error)

type AnonymousNode struct {
	nStreams int

	state0 values.Value
	state  values.Value
	do     NodeFunc
	inTs   []values.Type
	outT   values.Type
}

func NewNode(nStreams int, do func(collector Collector, v values.Value) error, types ...values.Type) *AnonymousNode {
	return NewStatefulNode(
		nStreams,
		values.NewNull(values.Int64),
		func(state values.Value, collector Collector, v values.Value) (value values.Value, e error) {
			return state, do(collector, v)
		},
		types...,
	)
}

func NewStatefulNode(nStreams int, state0 values.Value, do NodeFunc, types ...values.Type) *AnonymousNode {
	if len(types) < 2 {
		panic(fmt.Errorf("need at least 2 types, got: %v", types))
	}
	return &AnonymousNode{
		nStreams: nStreams,
		state0:   state0,
		state:    state0,
		do:       do,
		inTs:     types[:len(types)-1],
		outT:     types[len(types)-1],
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

func (o *AnonymousNode) InTypes() []values.Type {
	return o.inTs
}

func (o *AnonymousNode) OutType() values.Type {
	return o.outT
}

func (o *AnonymousNode) Clone() Node {
	return NewStatefulNode(
		o.nStreams,
		o.state0,
		o.do,
		append(o.inTs, o.outT)...)
}

// TODO(affo): should be done for multiple types?
func NewLogSink(t values.Type) (Node, *values.List) {
	s := values.NewList(t)
	return NewStatefulNode(1, s,
		func(state values.Value, collector Collector, v values.Value) (updatedState values.Value, e error) {
			err := state.(*values.List).AddValue(v)
			return state, err
		},
		// The out type is irrelevant.
		[]values.Type{t, values.Bool}...), s
}
