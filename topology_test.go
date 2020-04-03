package ssp

import (
	"strconv"
	"testing"

	"github.com/affo/ssp/values"
	"github.com/google/go-cmp/cmp"
)

var _ Node = (*BaseNode)(nil)

type BaseNode struct {
	*AnonymousNode
}

func Test_NewTopology(t *testing.T) {
	ctx := Context()
	ns := make([]Node, 8)
	for i := 0; i < len(ns); i++ {
		ns[i] = NewNode(func(collector Collector, v values.Value) error {
			return nil
		}).SetName(strconv.FormatInt(int64(i), 10))
	}

	o := ns[0].Out().Connect(ctx, ns[1]).Out()
	// Multiple out.
	o.Connect(ctx, ns[2])
	o.Connect(ctx, ns[3])
	// Multiple in.
	ns[2].Out().Connect(ctx, ns[4])
	ns[3].Out().Connect(ctx, ns[4])
	ns[4].Out().Connect(ctx, ns[5])
	// Disconnected piece.
	ns[6].Out().Connect(ctx, ns[7])

	g := GetGraph(ctx)
	want := `0 -> 1
1 -> 2
1 -> 3
2 -> 4
3 -> 4
4 -> 5
6 -> 7
`
	if diff := cmp.Diff(want, g.String()); diff != "" {
		t.Errorf("unexpected result -want/+got:\n\t%s", diff)
	}
}
