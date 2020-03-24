package ssp

import (
	"strings"
	"testing"

	"github.com/affo/ssp/values"
	"github.com/fortytw2/leaktest"
	"github.com/google/go-cmp/cmp"
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

	p := NewNode(0, func(collector Collector, vs ...values.Value) error {
		for i := 0; i < 5; i++ {
			collector.Collect(values.New(int64(i)))
		}
		return nil
	}, values.Bool, values.Int64).
		Out().
		Connect(ctx, NewStatefulNode(1, values.New(int64(0)),
			func(state values.Value, collector Collector, vs ...values.Value) (updatedState values.Value, e error) {
				state = values.New(state.Int64() + vs[0].Int64())
				collector.Collect(state)
				return state, e
			},
			values.Int64, values.Int64), FixedSteer()).Out()

	sink, log := NewLogSink(values.Int64)
	p.Connect(ctx, sink, FixedSteer())

	e := &Engine{}
	if err := e.Execute(ctx); err != nil {
		t.Fatal(err)
	}

	var got []int64
	for _, v := range log.GetValues() {
		got = append(got, v.Int64())
	}
	if diff := cmp.Diff([]int64{0, 1, 3, 6, 10}, got); diff != "" {
		t.Errorf("unexpected result -want/+got:\n\t%s", diff)
	}
}
