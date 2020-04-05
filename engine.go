package ssp

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"

	"github.com/affo/ssp/values"
)

func Execute(ctx context.Context) error {
	e := &Engine{}
	return e.Execute(ctx)
}

type Engine struct{}

func (e *Engine) Execute(ctx context.Context) error {
	g := GetGraph(ctx)
	ops := make(map[Node]*ParallelOperator)
	ins := make(map[Node][]Node)
	Walk(g, func(a *Arch) {
		if from := a.From(); from != nil {
			if _, ok := ops[from]; !ok {
				par := from.GetParallelism()
				ops[from] = NewParallelOperator(par, func() *Operator {
					return NewOperator(from)
				})
			}
		}
		if to := a.To(); to != nil {
			if _, ok := ops[to]; !ok {
				par := to.GetParallelism()
				ops[to] = NewParallelOperator(par, func() *Operator {
					return NewOperator(to)
				}, WithInKeySelector(a.ks))
			}
		}
		if from, to := a.From(), a.To(); from != nil && to != nil {
			if _, ok := ins[to]; !ok {
				ins[to] = make([]Node, 0, 1)
			}
			ins[to] = append(ins[to], from)
		}
	})

	outs := make(map[Node][]Collector)
	for n, in := range ins {
		inss := make([]*infiniteStream, 0, len(in))
		to := ops[n]
		for _, from := range in {
			is := NewInfiniteStream()
			inss = append(inss, is)
			if _, ok := outs[from]; !ok {
				outs[from] = make([]Collector, 0, 1)
			}
			outs[from] = append(outs[from], is)
		}
		to.In(newDataStreams(inss...), func() Transport {
			return NewInfiniteStream()
		})
	}

	for n, out := range outs {
		ops[n].Out(out)
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
	n     int64
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
		n:     int64(len(ss)),
	}
}

func (d *dataStreams) Next() values.Value {
	if n := atomic.LoadInt64(&d.n); n == 0 {
		for _, s := range d.ss {
			s.close()
		}
		return nil
	}
	i, value, ok := reflect.Select(d.cases)
	// !ok means the channel has been closed.
	if !ok {
		panic("unexpected close")
	}
	v := value.Interface().(values.Value)
	if v.Type() == values.Close {
		atomic.AddInt64(&d.n, -1)
		return d.Next()
	}
	return values.NewValueWithSource(values.Source(i), v)
}

// sharedCollector de-multiplies Close signals.
type sharedCollector struct {
	c   Collector
	par int64
}

func newSharedCollector(c Collector, par int) *sharedCollector {
	return &sharedCollector{
		c:   c,
		par: int64(par),
	}
}

func (s *sharedCollector) Collect(v values.Value) {
	if v.Type() == values.Close {
		par := atomic.AddInt64(&s.par, -1)
		if par > 0 {
			return
		}
	}
	s.c.Collect(v)
}

// broadcastCollector broadcasts values to multiple collectors.
type broadcastCollector struct {
	cs []Collector
}

func newBroadCastCollector(cs []Collector) broadcastCollector {
	return broadcastCollector{
		cs: cs,
	}
}

func (s broadcastCollector) Collect(v values.Value) {
	for _, c := range s.cs {
		c.Collect(v)
	}
}

type Operator struct {
	bn  Node
	ns  map[values.Key]Node
	in  DataStream
	out Collector

	wg  sync.WaitGroup
	err error
}

func NewOperator(n Node) *Operator {
	op := &Operator{
		bn: n,
		ns: make(map[values.Key]Node),
	}
	return op
}

func (o *Operator) In(ds DataStream) {
	o.in = ds
}

func (o *Operator) Out(c Collector) {
	o.out = c
}

func (o *Operator) getNode(key values.Key) Node {
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

	for {
		v := o.in.Next()
		if v == nil {
			return nil
		}
		n := o.getNode(values.GetKey(v))
		if err := n.Do(o.out, v); err != nil {
			return err
		}
	}
}

func (o *Operator) Open() {
	o.wg.Add(1)
	go func() {
		o.err = o.do()
		// Propagate close. Note that sinks have nil collector.
		if o.out != nil {
			SendClose(o.out)
		}
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

func (o *ParallelOperator) Out(cs []Collector) {
	bc := newBroadCastCollector(cs)
	sc := newSharedCollector(bc, len(o.ops))
	for _, o := range o.ops {
		o.Out(sc)
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
