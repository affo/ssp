package graph

import (
	"strconv"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func Test_NewGraph(t *testing.T) {
	ctx := Context()
	ns := make([]Node, 8)
	for i := 0; i < len(ns); i++ {
		ns[i] = BaseNode{ID: strconv.FormatInt(int64(i), 10)}
	}

	o := ns[0].Out().Connect(ctx, ns[1], "").Out()
	// Multiple out.
	o.Connect(ctx, ns[2], "")
	o.Connect(ctx, ns[3], "")
	// Multiple in.
	ns[2].Out().Connect(ctx, ns[4], "")
	ns[3].Out().Connect(ctx, ns[4], "")
	ns[4].Out().Connect(ctx, ns[5], "")
	// Disconnected piece.
	ns[6].Out().Connect(ctx, ns[7], "")

	g := GetGraph(ctx)

	if diff := cmp.Diff(map[Node]bool{
		ns[0]: true,
		ns[6]: true,
		ns[1]: false,
		ns[2]: false,
		ns[3]: false,
		ns[4]: false,
		ns[5]: false,
		ns[7]: false,
	}, g.roots); diff != "" {
		t.Errorf("unexpected node -want/+got:\n\t%s", diff)
	}

	got := make(map[Node][]Node)
	for from, arches := range g.adjacency {
		for _, arch := range arches {
			got[from] = append(got[from], arch.To())
		}
	}

	if diff := cmp.Diff(map[Node][]Node{
		ns[0]: {ns[1]},
		ns[1]: {ns[2], ns[3]},
		ns[2]: {ns[4]},
		ns[3]: {ns[4]},
		ns[4]: {ns[5]},
		ns[6]: {ns[7]},
	}, got); diff != "" {
		t.Errorf("unexpected node -want/+got:\n\t%s", diff)
	}
}

func Test_String(t *testing.T) {
	ctx := Context()
	BaseNode{ID: "1"}.
		Out().Connect(ctx, BaseNode{ID: "2"}, "arch12").
		Out().Connect(ctx, BaseNode{ID: "3"}, "arch23")
	g := GetGraph(ctx)
	want := `1 -> 2 [name: arch12]
2 -> 3 [name: arch23]
`
	if diff := cmp.Diff(want, g.String()); diff != "" {
		t.Errorf("unexpected result -want/+got:\n\t%s", diff)
	}
}

func Test_MultipleOut(t *testing.T) {
	ctx := Context()
	o := BaseNode{ID: "1"}.Out()
	o.Connect(ctx, BaseNode{ID: "2"}, "")
	o.Connect(ctx, BaseNode{ID: "3"}, "")
	g := GetGraph(ctx)
	want := `1 -> 2 [name: ]
1 -> 3 [name: ]
`
	if diff := cmp.Diff(want, g.String()); diff != "" {
		t.Errorf("unexpected result -want/+got:\n\t%s", diff)
	}
}

func Test_MultipleIn(t *testing.T) {
	ctx := Context()
	sink := BaseNode{ID: "sink"}
	BaseNode{ID: "1"}.Out().Connect(ctx, sink, "")
	BaseNode{ID: "2"}.Out().Connect(ctx, sink, "")
	g := GetGraph(ctx)
	want := `1 -> sink [name: ]
2 -> sink [name: ]
`
	if diff := cmp.Diff(want, g.String()); diff != "" {
		t.Errorf("unexpected result -want/+got:\n\t%s", diff)
	}
}

func Test_Cycle(t *testing.T) {
	ctx := Context()
	n1 := BaseNode{ID: "1"}
	n2 := BaseNode{ID: "2"}
	n1.Out().Connect(ctx, n2, "")
	n2.Out().Connect(ctx, n1, "")
	g := GetGraph(ctx)
	want := `1 -> 2 [name: ]
2 -> 1 [name: ]
`
	if diff := cmp.Diff(want, g.String()); diff != "" {
		t.Errorf("unexpected result -want/+got:\n\t%s", diff)
	}
}
