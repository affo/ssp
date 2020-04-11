package values

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

var v = NewKeyedValue(Key(0),
	NewValueWithSource(Source(0),
		NewKeyedValue(Key(1),
			NewKeyedValue(Key(2),
				NewValueWithSource(Source(1),
					NewKeyedValue(Key(3),
						NewValueWithSource(Source(2),
							New(42))))))))

func TestGetKey(t *testing.T) {
	got := GetKey(v)
	want := Key(0)
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("unexpected result -want/+got:\n\t%s", diff)
	}
	got = GetKey(v.Unwrap())
	want = Key(1)
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("unexpected result -want/+got:\n\t%s", diff)
	}
	got = GetKey(v.Unwrap().Unwrap())
	want = Key(1)
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("unexpected result -want/+got:\n\t%s", diff)
	}
	got = GetKey(v.Unwrap().Unwrap().Unwrap())
	want = Key(2)
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("unexpected result -want/+got:\n\t%s", diff)
	}
	got = GetKey(v.Unwrap().Unwrap().Unwrap().Unwrap())
	want = Key(3)
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("unexpected result -want/+got:\n\t%s", diff)
	}
	got = GetKey(v.Unwrap().Unwrap().Unwrap().Unwrap().Unwrap())
	want = Key(3)
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("unexpected result -want/+got:\n\t%s", diff)
	}
}

func TestGetSource(t *testing.T) {
	got := GetSource(v)
	want := Source(0)
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("unexpected result -want/+got:\n\t%s", diff)
	}
	got = GetSource(v.Unwrap())
	want = Source(0)
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("unexpected result -want/+got:\n\t%s", diff)
	}
	got = GetSource(v.Unwrap().Unwrap())
	want = Source(1)
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("unexpected result -want/+got:\n\t%s", diff)
	}
	got = GetSource(v.Unwrap().Unwrap().Unwrap())
	want = Source(1)
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("unexpected result -want/+got:\n\t%s", diff)
	}
	got = GetSource(v.Unwrap().Unwrap().Unwrap().Unwrap())
	want = Source(1)
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("unexpected result -want/+got:\n\t%s", diff)
	}
	got = GetSource(v.Unwrap().Unwrap().Unwrap().Unwrap().Unwrap())
	want = Source(2)
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("unexpected result -want/+got:\n\t%s", diff)
	}
	got = GetSource(v.Unwrap().Unwrap().Unwrap().Unwrap().Unwrap().Unwrap())
	want = Source(2)
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("unexpected result -want/+got:\n\t%s", diff)
	}
}
