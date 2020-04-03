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
	ops := make(map[Node]*ParallelOperator)
	ins := make(map[Node][]*infiniteStream)
	outs := make(map[Node][]*infiniteStream)
	Walk(g, func(s *Arch) {
		if from := s.From(); from != nil {
			if _, ok := ops[from]; !ok {
				is := NewInfiniteStream()
				ops[from] = NewParallelOperator(from.GetParallelism(), func() *Operator {
					return NewOperator(from, is)
				})
				if _, ok := outs[from]; !ok {
					outs[from] = make([]*infiniteStream, 0)
				}
				outs[from] = append(outs[from], is)
			}
		}
		if to := s.To(); to != nil {
			if _, ok := ops[to]; !ok {
				is := NewInfiniteStream()
				ops[to] = NewParallelOperator(to.GetParallelism(), func() *Operator {
					return NewOperator(to, is)
				}, WithInKeySelector(s.ks))
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
		ops[n].In(newDataStreams(in...), func() Transport {
			return NewInfiniteStream()
		})
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
	ss    []*infiniteStream
	cases []reflect.SelectCase
}

func newDataStreams(ss ...*infiniteStream) *dataStreams {
	cases := make([]reflect.SelectCase, len(ss))
	for i, s := range ss {
		cases[i] = reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(s.s),
		}
	}
	return &dataStreams{
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

type Operator struct {
	bn  Node
	ns  map[uint64]Node
	in  DataStream
	out Collector

	wg  sync.WaitGroup
	err error
}

func NewOperator(n Node, out Collector) *Operator {
	op := &Operator{
		bn:  n,
		ns:  make(map[uint64]Node),
		out: out,
	}
	return op
}

func (o *Operator) In(ds DataStream) {
	o.in = ds
}

func (o *Operator) getNode(key values.Key) Node {
	if _, ok := o.ns[uint64(key)]; !ok {
		o.ns[uint64(key)] = o.bn.Clone()
	}
	return o.ns[uint64(key)]
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
			// The keyed value has already been prepared for us.
			n = o.getNode(kv.Key())
		} else {
			// Pick the first node available:
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

type operatorOptions struct {
	inKs KeySelector
}

type OperatorOption func(options *operatorOptions)

func WithInKeySelector(ks KeySelector) OperatorOption {
	return func(o *operatorOptions) {
		o.inKs = ks
	}
}

type ParallelOperator struct {
	ops  []*Operator
	opts operatorOptions
}

func NewParallelOperator(par int, f func() *Operator, opts ...OperatorOption) *ParallelOperator {
	ops := make([]*Operator, par)
	for i := 0; i < len(ops); i++ {
		ops[i] = f()
	}
	pop := &ParallelOperator{
		ops: ops,
	}
	for _, opt := range opts {
		opt(&pop.opts)
	}
	return pop
}

func (o *ParallelOperator) In(ds DataStream, f func() Transport) {
	ps := NewPartitionedStream(len(o.ops), o.opts.inKs, ds, f)
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
	ds DataStream
	ts []Transport
	ks KeySelector
}

func NewPartitionedStream(par int, ks KeySelector, ds DataStream, f func() Transport) *partitionedStream {
	if ks == nil {
		ks = NewRoundRobinKeySelector(par)
	}
	ts := make([]Transport, par)
	for i := 0; i < len(ts); i++ {
		ts[i] = f()
	}
	ps := &partitionedStream{
		ds: ds,
		ts: ts,
		ks: ks,
	}
	go ps.do()
	return ps
}

func (s *partitionedStream) Stream(partition int) DataStream {
	return s.ts[partition]
}

func (s *partitionedStream) do() {
	for v := s.ds.Next(); v != nil; v = s.ds.Next() {
		if kv, ok := v.(values.KeyedValue); ok {
			// Remove previous keying if any.
			v = kv.Unwrap()
		}
		// Apply new keying.
		k := s.ks.GetKey(v)
		kv := values.NewKeyedValue(k, v)
		i := uint64(kv.Key()) % uint64(len(s.ts))
		t := s.ts[i]
		t.Collect(kv)
	}
	for _, t := range s.ts {
		SendClose(t)
	}
}
