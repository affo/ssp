package ssp

import (
	"context"
	"fmt"
	"reflect"
	"sync"

	"github.com/affo/ssp/values"
)

type Engine struct{}

func (e *Engine) Execute(ctx context.Context) error {
	g := GetGraph(ctx)
	size := 1024
	ops := make(map[Node]*Operator)
	ins := make(map[Node][]*infiniteStream)
	outs := make(map[Node][]*infiniteStream)
	Walk(g, func(s Stream) {
		if from := s.From(); from != nil {
			if _, ok := ops[from]; !ok {
				is := NewInfiniteStream(from.OutType(), WithBuffer(size), WithSteer(s.Steer()))
				ops[from] = NewOperator(from, is)
				if _, ok := outs[from]; !ok {
					outs[from] = make([]*infiniteStream, 0)
				}
				outs[from] = append(outs[from], is)
			}
		}
		if to := s.To(); to != nil {
			if _, ok := ops[to]; !ok {
				is := NewInfiniteStream(to.OutType(), WithBuffer(size), WithSteer(s.Steer()))
				ops[to] = NewOperator(to, is)
				if _, ok := outs[to]; !ok {
					outs[to] = make([]*infiniteStream, 0)
				}
				outs[to] = append(outs[to], is)
			}
		}
		if from, to := s.From(), s.To(); from != nil && to != nil {
			if _, ok := ins[to]; !ok {
				ins[to] = make([]*infiniteStream, 0)
			}
			ins[to] = outs[from]
		}
	})

	for n, in := range ins {
		ops[n].In(newDataStreams(in...))
	}

	for _, op := range ops {
		op.Open()
	}
	var werr error
	for _, op := range ops {
		if err := op.Close(); err != nil {
			werr = fmt.Errorf("error on operator close: %w", err)
		}
	}
	return werr
}

type dataStreams struct {
	t     values.Type
	ss    []*infiniteStream
	cases []reflect.SelectCase
}

func newDataStreams(ss ...*infiniteStream) *dataStreams {
	var t values.Type
	cases := make([]reflect.SelectCase, len(ss))
	for i, s := range ss {
		t = s.t
		cases[i] = reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(s.s),
		}
	}
	return &dataStreams{
		t:     t,
		ss:    ss,
		cases: cases,
	}
}

func (d dataStreams) Next() values.Value {
	_, value, ok := reflect.Select(d.cases)
	// ok will be true if the channel has not been closed.
	if !ok {
		return nil
	}
	v := value.Interface().(values.Value)
	if v.Type() == values.Close {
		for _, s := range d.ss {
			s.close()
		}
		return nil
	}
	return v
}

func (d dataStreams) Type() values.Type {
	return d.t
}

type Operator struct {
	bn  Node
	ns  map[int]Node
	in  DataStream
	out Collector

	wg  sync.WaitGroup
	err error
}

func NewOperator(n Node, out Collector) *Operator {
	return &Operator{
		bn:  n,
		ns:  make(map[int]Node),
		out: out,
	}
}

func (o *Operator) In(ds DataStream) {
	o.in = ds
}

func (o *Operator) getNode(key int) Node {
	if _, ok := o.ns[key]; !ok {
		o.ns[key] = o.bn.Clone()
	}
	return o.ns[key]
}

func (o *Operator) do() error {
	// This is a source, the provided value is useless.
	if o.in == nil {
		return o.bn.Do(o.out, values.NewNull(values.Int64))
	}

	var stop bool
	for !stop {
		v := o.in.Next()
		var n Node
		if kv, ok := v.(values.KeyedValue); ok {
			n = o.getNode(kv.Key())
		} else {
			n = o.getNode(0)
		}
		if v != nil {
			if err := n.Do(o.out, v); err != nil {
				return err
			}
		} else {
			stop = true
		}
	}
	return nil
}

func (o *Operator) Open() {
	o.wg.Add(1)
	go func() {
		o.err = o.do()
		// Propagate close.
		SendClose(o.out)
		o.wg.Done()
	}()
}

func (o *Operator) Close() error {
	o.wg.Wait()
	return o.err
}

type ParallelOperator struct {
	ops []*Operator
}

func NewParallelOperator(par int, f func() *Operator) *ParallelOperator {
	ops := make([]*Operator, par)
	for i := 0; i < len(ops); i++ {
		ops[i] = f()
	}
	return &ParallelOperator{
		ops: ops,
	}
}

func (o *ParallelOperator) In(ts Transport) {
	ps := NewPartitionedStream(ts, len(o.ops))
	for i, o := range o.ops {
		o.In(ps.Stream(i))
	}
}

func (o *ParallelOperator) Open() {
	for _, op := range o.ops {
		op.Open()
	}
}

func (o *ParallelOperator) Close() error {
	var err error
	for _, op := range o.ops {
		if oerr := op.Close(); oerr != nil {
			err = oerr
		}
	}
	return err
}

type partitionedStream struct {
	ds Transport
	ts []Transport
}

func NewPartitionedStream(t Transport, par int) *partitionedStream {
	ts := make([]Transport, par)
	for i := 0; i < len(ts); i++ {
		ts[i] = t.Clone()
	}
	ps := &partitionedStream{
		ds: t,
		ts: ts,
	}
	go ps.do()
	return ps
}

func (s *partitionedStream) Stream(partition int) DataStream {
	return s.ts[partition]
}

func (s *partitionedStream) do() {
	for v := s.ds.Next(); v != nil; v = s.ds.Next() {
		kv := v.(values.KeyedValue)
		i := kv.Key() % len(s.ts)
		t := s.ts[i]
		t.Collect(v)
	}
	for _, t := range s.ts {
		SendClose(t)
	}
}
