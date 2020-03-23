package ssp

import (
	"strings"
	"testing"

	"github.com/affo/ssp/values"
	"github.com/fortytw2/leaktest"
)

func TestOperator(t *testing.T) {
	defer leaktest.Check(t)()

	n := NewNode(1, func(collector Collector, vs ...values.Value) error {
		collector.Collect(values.New(strings.ToUpper(vs[0].String())))
		return nil
	}, values.String, values.String)
	in := NewInfiniteStream(values.String, 10)
	out := NewInfiniteStream(values.String, 10)
	o := NewOperator(n, out)
	o.In(in)
	o.Open()
	defer func() {
		in.Close()
		out.Close()
		if err := o.Close(); err != nil {
			t.Fatalf("unexpected error on close: %v", err)
		}
	}()

	in.Collect(values.New("hello"))
	in.Collect(values.New("this"))
	in.Collect(values.New("is"))
	in.Collect(values.New("ssp"))

	want := []string{"HELLO", "THIS", "IS", "SSP"}
	for i := 0; i < len(want); i++ {
		if got := out.Next().String(); want[i] != got {
			t.Errorf("expected %s got %v", want[i], got)
		}
	}
}

func TestEngine(t *testing.T) {
	defer leaktest.Check(t)()

	ctx := Context()

	NewNode(0, func(collector Collector, vs ...values.Value) error {
		for i := 0; i < 10; i++ {
			collector.Collect(values.New(int64(i)))
		}
		return nil
	}, values.Bool, values.Int64).
		Out().
		Connect(ctx, NewStatefulNode(1, values.New(int64(1)),
			func(state values.Value, collector Collector, vs ...values.Value) (updatedState values.Value, e error) {
				state = values.New(state.Int64() + 1)
				collector.Collect(state)
				return state, e
			},
			values.Int64, values.Int64), FixedSteer())

	e := &Engine{}
	if err := e.Execute(ctx); err != nil {
		t.Fatal(err)
	}
}
