package ssp

import (
	"context"
	"fmt"
	"sync"

	"github.com/affo/ssp/values"
)

type Engine struct{}

func (e *Engine) Execute(ctx context.Context) error {
	g := GetGraph(ctx)
	size := 1024
	ops := make(map[Node]*Operator)
	Walk(g, func(s Stream) {
		if from := s.From(); from != nil {
			if _, ok := ops[from]; !ok {
				ops[from] = NewOperator(from, NewInfiniteStream(from.OutType(), size))
			}
		}
		if to := s.To(); to != nil {
			if _, ok := ops[to]; !ok {
				ops[to] = NewOperator(to, NewInfiniteStream(to.OutType(), size))
			}
		}
		if from, to := s.From(), s.To(); from != nil && to != nil {
			ops[to].In(ops[from].c)
		}
	})

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

type Operator struct {
	n  Node
	ds []DataStream
	c  TwoWayStream

	wg  sync.WaitGroup
	err error
}

func NewOperator(n Node, out TwoWayStream) *Operator {
	return &Operator{
		n: n,
		c: out,
	}
}

func (o *Operator) In(ds DataStream) {
	o.ds = append(o.ds, ds)
}

func (o *Operator) do() error {
	// This is a source.
	if len(o.ds) == 0 {
		return o.n.Do(o.c)
	}

	var stop bool
	for !stop {
		vs := make([]values.Value, len(o.ds))
		for i, d := range o.ds {
			vs[i] = d.Next()
		}
		// Stop condition.
		stop = true
		for i, v := range vs {
			if v == nil {
				vs[i] = values.NewNull(o.ds[i].Type())
			} else {
				stop = false
			}
		}
		if !stop {
			if err := o.n.Do(o.c, vs...); err != nil {
				return err
			}
		}
	}
	return nil
}

func (o *Operator) Open() {
	// TODO(affo): there will be parallelism here.
	o.wg.Add(1)
	go func() {
		o.err = o.do()
		o.c.Collect(values.NewMeta(values.Close))
		o.wg.Done()
	}()
}

func (o *Operator) Close() error {
	o.wg.Wait()
	return o.err
}
