package naive

import (
	"testing"

	"github.com/affo/ssp"
	"github.com/google/go-cmp/cmp"
)

func Test_Execute(t *testing.T) {
	ctx := ssp.Context()

	in := NewStreamFromElements(NewIntValues(1, 2, 3, 4, 5)...)
	o := NewSource(in).
		Connect(ctx, NewMapper(func(v ssp.Value) []ssp.Value {
			return []ssp.Value{ssp.NewValue(ssp.GetInt(v) * 2)}
		}), ssp.FixedSteer()).Out()
	s := &Sink{}
	o.Connect(ctx, s, ssp.FixedSteer())

	if err := Execute(ctx); err != nil {
		t.Fatal(err)
	}

	got := make([]int64, 0, len(s.Values))
	for _, v := range s.Values {
		got = append(got, ssp.GetInt(v))
	}
	if diff := cmp.Diff([]int64{2, 4, 6, 8, 10}, got); diff != "" {
		t.Errorf("unexpected result -want/+got:\n\t%s", diff)
	}
}
