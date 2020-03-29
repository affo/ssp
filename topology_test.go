package ssp

import (
	"strconv"
	"testing"

	"github.com/affo/ssp/values"
	"github.com/google/go-cmp/cmp"
)

var _ Node = (*BaseNode)(nil)

type BaseNode struct {
	ID string
}

func (n BaseNode) Out() Stream {
	return NewStream(n)
}

func (n BaseNode) Do(collector Collector, v values.Value) error {
	panic("implement me")
}

func (n BaseNode) InTypes() []values.Type {
	return []values.Type{values.Int64}
}

func (n BaseNode) OutType() values.Type {
	return values.Int64
}

func (n BaseNode) String() string {
	return n.ID
}

func (n BaseNode) Clone() Node {
	// No need to clone here.
	return n
}

func Test_NewTopology(t *testing.T) {
	ctx := Context()
	ns := make([]Node, 8)
	for i := 0; i < len(ns); i++ {
		ns[i] = BaseNode{ID: strconv.FormatInt(int64(i), 10)}
	}

	o := ns[0].Out().Connect(ctx, ns[1], FixedSteer()).Out()
	// Multiple out.
	o.Connect(ctx, ns[2], FixedSteer())
	o.Connect(ctx, ns[3], FixedSteer())
	// Multiple in.
	ns[2].Out().Connect(ctx, ns[4], FixedSteer())
	ns[3].Out().Connect(ctx, ns[4], FixedSteer())
	ns[4].Out().Connect(ctx, ns[5], FixedSteer())
	// Disconnected piece.
	ns[6].Out().Connect(ctx, ns[7], FixedSteer())

	g := GetGraph(ctx)
	want := `0 -> 1 [steer: fixed]
1 -> 2 [steer: fixed]
1 -> 3 [steer: fixed]
2 -> 4 [steer: fixed]
3 -> 4 [steer: fixed]
4 -> 5 [steer: fixed]
6 -> 7 [steer: fixed]
`
	if diff := cmp.Diff(want, g.String()); diff != "" {
		t.Errorf("unexpected result -want/+got:\n\t%s", diff)
	}
}
