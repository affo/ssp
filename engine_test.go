package ssp

import (
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/affo/ssp/values"
	"github.com/fortytw2/leaktest"
	"github.com/google/go-cmp/cmp"
)

func TestOperator(t *testing.T) {
	defer leaktest.Check(t)()

	n := NewNode(func(collector Collector, v values.Value) error {
		collector.Collect(values.New(strings.ToUpper(v.String())))
		return nil
	})
	in := NewInfiniteStream()
	out := NewInfiniteStream()
	// Need some buffering to avoid deadlock because we consume at the end.
	out.bufferSize = 10
	o := NewOperator(n, out)
	o.In(in)
	o.Open()
	defer func() {
		if err := o.Close(); err != nil {
			t.Fatalf("unexpected error on close: %v", err)
		}
	}()

	in.Collect(values.New("hello"))
	in.Collect(values.New("this"))
	in.Collect(values.New("is"))
	in.Collect(values.New("ssp"))
	SendClose(in)

	want := []string{"HELLO", "THIS", "IS", "SSP"}
	for i := 0; i < len(want); i++ {
		if got := out.Next().String(); want[i] != got {
			t.Errorf("expected %s got %v", want[i], got)
		}
	}
}

func TestParallelOperator(t *testing.T) {
	defer leaktest.Check(t)()

	ks := NewStringValueKeySelector(func(v values.Value) string {
		return v.String()
	})
	in := NewInfiniteStream()
	out := NewInfiniteStream()
	// Need some buffering to avoid deadlock because we consume at the end.
	out.bufferSize = 10
	o := NewParallelOperator(4, func() *Operator {
		return NewOperator(NewStatefulNode(values.New(int64(0)),
			func(state values.Value, collector Collector, v values.Value) (values.Value, error) {
				count := state.Int64() + 1
				collector.Collect(values.New(fmt.Sprintf("%v: %d", v, count)))
				return values.New(count), nil
			}), out)
	}, WithInKeySelector(ks))
	o.In(in, func() Transport {
		return NewInfiniteStream()
	})
	o.Open()
	defer func() {
		SendClose(in)
		if err := o.Close(); err != nil {
			t.Fatalf("unexpected error on close: %v", err)
		}
	}()

	ins := []string{
		"hello",
		"this",
		"is",
		"ssp",
		"hello",
		"this",
		"is",
		"sparta",
		"sparta",
		"is",
		"leonida",
	}

	for _, i := range ins {
		in.Collect(values.New(i))
	}

	want := []string{
		"hello: 1",
		"hello: 2",
		"is: 1",
		"is: 2",
		"is: 3",
		"leonida: 1",
		"sparta: 1",
		"sparta: 2",
		"ssp: 1",
		"this: 1",
		"this: 2",
	}
	got := make([]string, len(ins))

	for i := 0; i < len(got); i++ {
		got[i] = out.Next().String()
	}
	sort.Strings(got)

	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("unexpected reesult:\n\t%s", diff)
	}
}

func TestEngine(t *testing.T) {
	defer leaktest.Check(t)()

	ctx := Context()
	p := NewNode(func(collector Collector, v values.Value) error {
		for i := 0; i < 5; i++ {
			collector.Collect(values.New(int64(i)))
		}
		return nil
	}).
		Out().
		Connect(ctx, NewStatefulNode(values.New(int64(0)),
			func(state values.Value, collector Collector, v values.Value) (updatedState values.Value, e error) {
				state = values.New(state.Int64() + v.Int64())
				collector.Collect(state)
				return state, e
			}), NewFixedKeySelector()).Out()

	sink, log := NewLogSink(values.Int64)
	p.Connect(ctx, sink, NewFixedKeySelector())

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
