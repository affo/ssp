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
	o := NewNode(func(collector Collector, v values.Value) error {
		i := v.Int64()
		collector.Collect(values.New(i * 1))
		collector.Collect(values.New(i * 2))
		collector.Collect(values.New(i * 3))
		collector.Collect(values.New(i * 4))
		return nil
	})
	c := &dumbCollector{}
	if err := o.Do(c, values.New(int64(1))); err != nil {
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
	o := NewNode(func(collector Collector, v values.Value) error {
		return fmt.Errorf("an error")
	})
	// Pass at least 1 value to trigger the error above.
	if err := o.Do(&dumbCollector{}, values.New(int64(1))); err == nil {
		t.Fatal("expected error got none")
	}
}
