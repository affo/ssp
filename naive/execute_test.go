package naive

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

func Test_Execute(t *testing.T) {
	ctx := Context()

	in := NewStreamFromElements(1, 2, 3, 4, 5)
	o := NewSource(in).
		Connect(ctx, NewMapper(func(v int) []int {
			return []int{v * 2}
		})).Out()
	s := &Sink{}
	o.Connect(ctx, s)

	if err := Execute(ctx); err != nil {
		t.Fatal(err)
	}

	got := make([]int, 0, len(s.Values))
	for _, v := range s.Values {
		got = append(got, v)
	}
	if diff := cmp.Diff([]int{2, 4, 6, 8, 10}, got); diff != "" {
		t.Errorf("unexpected result -want/+got:\n\t%s", diff)
	}
}
