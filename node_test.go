package ssp

import (
	"fmt"
	"testing"

	"github.com/affo/ssp/values"
	"github.com/google/go-cmp/cmp"
)

type dumbCollector struct {
	vs []values.Value
}

func (c *dumbCollector) Collect(v values.Value) {
	c.vs = append(c.vs, v)
}

func Test_Node(t *testing.T) {
	o := NewNode(1, func(collector Collector, vs ...values.Value) error {
		v := vs[0].Int64()
		collector.Collect(values.New(v * 1))
		collector.Collect(values.New(v * 2))
		collector.Collect(values.New(v * 3))
		collector.Collect(values.New(v * 4))
		return nil
	}, values.Int64, values.Int64)
	c := &dumbCollector{}
	if err := o.Do(c, NewIntValues(1)...); err != nil {
		t.Fatal(err)
	}
	var got []int64
	for _, v := range c.vs {
		got = append(got, v.Int64())
	}
	if diff := cmp.Diff([]int64{1, 2, 3, 4}, got); diff != "" {
		t.Errorf("unexpected result -want/+got:\n\t%s", diff)
	}
}

func Test_Node_Error(t *testing.T) {
	t.Run("exceed input streams", func(t *testing.T) {
		defer func() {
			if err := recover(); err == nil {
				t.Fatal("did not panic")
			}
		}()

		o := NewNode(1, func(collector Collector, vs ...values.Value) error {
			return nil
		}, values.Int64, values.Int64)
		_ = o.Do(&dumbCollector{}, NewIntValues(1, 1)...)
	})

	t.Run("omit types", func(t *testing.T) {
		defer func() {
			if err := recover(); err == nil {
				t.Fatal("did not panic")
			}
		}()

		o := NewNode(1, func(collector Collector, vs ...values.Value) error {
			return nil
		}, values.Int64)
		_ = o.Do(&dumbCollector{}, NewIntValues(1)...)
	})

	t.Run("do error", func(t *testing.T) {
		o := NewNode(1, func(collector Collector, vs ...values.Value) error {
			return fmt.Errorf("an error")
		}, values.Int64, values.Int64)
		// Pass at least 1 value to trigger the error above.
		if err := o.Do(&dumbCollector{}, NewIntValues(1)...); err == nil {
			t.Fatal("expected error got none")
		}
	})
}
