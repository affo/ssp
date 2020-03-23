package ssp

import (
	"fmt"

	"github.com/affo/ssp/values"
)

type Node interface {
	Out() Stream
	Do(collector Collector, vs ...values.Value) error
	InTypes() []values.Type
	OutType() values.Type
}

type NodeFunc func(state values.Value, collector Collector, vs ...values.Value) (updatedState values.Value, e error)

type AnonymousNode struct {
	nStreams int

	state values.Value
	do    NodeFunc
	inTs  []values.Type
	outT  values.Type
}

func NewNode(nStreams int, do func(collector Collector, vs ...values.Value) error, types ...values.Type) *AnonymousNode {
	return NewStatefulNode(
		nStreams,
		values.NewNull(values.Int64),
		func(state values.Value, collector Collector, vs ...values.Value) (value values.Value, e error) {
			return state, do(collector, vs...)
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
		state:    state0,
		do:       do,
		inTs:     types[:len(types)-2],
		outT:     types[len(types)-1],
	}
}

func (o *AnonymousNode) Out() Stream {
	return NewStream(o)
}

func (o *AnonymousNode) Do(collector Collector, vs ...values.Value) error {
	if len(vs) > o.nStreams {
		panic(fmt.Sprintf("cannot process streams: maximum number of streams is %d", o.nStreams))
	}
	s, err := o.do(o.state, collector, vs...)
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
