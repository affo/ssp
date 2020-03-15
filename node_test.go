package ssp

import (
	"fmt"
	"testing"

	"github.com/affo/ssp/values"
	"github.com/fortytw2/leaktest"
	"github.com/google/go-cmp/cmp"
)

type collector struct {
	vs []values.Value
}

func (c *collector) Collect(v values.Value) {
	c.vs = append(c.vs, v)
}

func Test_Operator(t *testing.T) {
	defer leaktest.Check(t)()

	ds := NewStreamFromElements(NewIntValues(1)...)
	o := NewOperator(1, func(collector Collector, vs ...values.Value) error {
		v := vs[0].Int()
		collector.Collect(values.NewValue(v * 1))
		collector.Collect(values.NewValue(v * 2))
		collector.Collect(values.NewValue(v * 3))
		collector.Collect(values.NewValue(v * 4))
		return nil
	})
	o.In(ds)
	c := &collector{}
	o.Open(c)
	if err := o.Close(); err != nil {
		t.Fatal(err)
	}
	var got []int
	for _, v := range c.vs {
		got = append(got, v.Int())
	}
	if diff := cmp.Diff([]int{1, 2, 3, 4}, got); diff != "" {
		t.Errorf("unexpected result -want/+got:\n\t%s", diff)
	}
}

func Test_Operator_Error(t *testing.T) {
	t.Run("exceed input streams", func(t *testing.T) {
		defer func() {
			defer leaktest.Check(t)()

			if err := recover(); err == nil {
				t.Fatal("did not panic")
			}
		}()

		o := NewOperator(1, func(collector Collector, vs ...values.Value) error {
			return nil
		})
		o.In(NewEmptyStream(values.Int))
		o.In(NewEmptyStream(values.Bool))
	})

	t.Run("do error", func(t *testing.T) {
		defer leaktest.Check(t)()

		o := NewOperator(1, func(collector Collector, vs ...values.Value) error {
			return fmt.Errorf("an error")
		})
		// Pass at least 1 value to trigger the error above.
		o.In(NewStreamFromElements(NewIntValues(1)...))
		c := &collector{}
		o.Open(c)
		if err := o.Close(); err == nil {
			t.Fatal("expected error got none")
		}
	})
}
