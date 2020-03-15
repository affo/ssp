package ssp

import (
	"fmt"
	"sync"

	"github.com/affo/ssp/values"
)

type Node interface {
	Out() Stream
	In(ds DataStream)
	Do(collector Collector) error
}

type Operator struct {
	ds       []DataStream
	nStreams int

	do func(collector Collector, vs ...values.Value) error

	wg  sync.WaitGroup
	err error
}

func NewOperator(nStreams int, do func(collector Collector, vs ...values.Value) error) *Operator {
	return &Operator{nStreams: nStreams, do: do}
}

func (o *Operator) Out() Stream {
	return NewStream(o)
}

func (o *Operator) In(ds DataStream) {
	if len(o.ds) >= o.nStreams {
		panic(fmt.Sprintf("cannot add input stream to operator: maximum number of streams is %d", o.nStreams))
	}
	o.ds = append(o.ds, ds)
}

func (o *Operator) Do(collector Collector) error {
	vs := make([]values.Value, len(o.ds))
	for i, d := range o.ds {
		if d.More() {
			vs[i] = d.Next()
		} else {
			vs[i] = values.NewNull(d.Type())
		}
	}
	// Stop condition.
	stop := true
	for _, v := range vs {
		if !v.IsNull() {
			stop = false
			break
		}
	}
	if stop {
		return nil
	}
	return o.do(collector, vs...)
}

func (o *Operator) Open(collector Collector) {
	// TODO(affo): there will be parallelism here.
	o.wg.Add(1)
	go func() {
		o.err = o.Do(collector)
		o.wg.Done()
	}()
}

func (o *Operator) Close() error {
	o.wg.Wait()
	return o.err
}
