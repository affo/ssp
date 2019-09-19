package graph

import (
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
)

type node struct {
	ID string
}

func (n node) Out() Arch {
	return NewArch(n)
}

func (n node) String() string {
	return fmt.Sprintf("test node %s", n.ID)
}

func Test_NewGraph(t *testing.T) {
	ctx := Context()
	n1 := node{}
	n2 := node{}
	out := n1.Out().Connect(ctx, n2).Out()
	g := GetGraph(ctx)
	if diff := cmp.Diff(n1, g.arches[0].From()); diff != "" {
		t.Errorf("unexpected node -want/+got:\n\t%s", diff)
	}
	if diff := cmp.Diff(n2, g.arches[0].To()); diff != "" {
		t.Errorf("unexpected node -want/+got:\n\t%s", diff)
	}
	if diff := cmp.Diff(n2, out.From()); diff != "" {
		t.Errorf("unexpected node -want/+got:\n\t%s", diff)
	}
	if sink := out.To(); sink != nil {
		t.Errorf("unexpected nil sink node:\n\t%v", sink)
	}
}

func Test_String(t *testing.T) {
	ctx := Context()
	node{ID: "1"}.
		Out().Connect(ctx, node{ID: "2"}).
		Out().Connect(ctx, node{ID: "3"})
	g := GetGraph(ctx)
	want := `test node 1 -> test node 2
test node 2 -> test node 3
`
	if diff := cmp.Diff(want, g.String()); diff != "" {
		t.Errorf("unexpected result -want/+got:\n\t%s", diff)
	}
}

func Test_MultipleOut(t *testing.T) {
	ctx := Context()
	o := node{ID: "1"}.Out()
	o.Connect(ctx, node{ID: "2"})
	o.Connect(ctx, node{ID: "3"})
	g := GetGraph(ctx)
	want := `test node 1 -> test node 2
test node 1 -> test node 3
`
	if diff := cmp.Diff(want, g.String()); diff != "" {
		t.Errorf("unexpected result -want/+got:\n\t%s", diff)
	}
}
