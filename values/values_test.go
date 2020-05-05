package values

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

func MustUnwrap(v Value, times int) Value {
	for ; times > 0; times-- {
		uv, err := v.Unwrap()
		if err != nil {
			panic(err)
		}
		v = uv
	}
	return v
}

func TestKey(t *testing.T) {
	t.Run("outer key", func(t *testing.T) {
		v := SetKey(Key(1), SetSource(Source(0), New(42)))
		got, _ := GetKey(v)
		want := Key(1)
		if diff := cmp.Diff(want, got); diff != "" {
			t.Errorf("unexpected result -want/+got:\n\t%s", diff)
		}
		_, err := GetKey(MustUnwrap(v, 1))
		if err == nil {
			t.Errorf("expected err, got none")
		}

		v = SetKey(Key(2), v)
		got, _ = GetKey(v)
		want = Key(2)
		if diff := cmp.Diff(want, got); diff != "" {
			t.Errorf("unexpected result -want/+got:\n\t%s", diff)
		}
		_, err = GetKey(MustUnwrap(v, 1))
		if err == nil {
			t.Errorf("expected err, got none")
		}
	})

	t.Run("inner key", func(t *testing.T) {
		v := SetSource(Source(0), SetKey(Key(1), New(42)))
		got, _ := GetKey(v)
		want := Key(1)
		if diff := cmp.Diff(want, got); diff != "" {
			t.Errorf("unexpected result -want/+got:\n\t%s", diff)
		}

		v = SetKey(Key(2), v)
		got, _ = GetKey(v)
		want = Key(2)
		if diff := cmp.Diff(want, got); diff != "" {
			t.Errorf("unexpected result -want/+got:\n\t%s", diff)
		}
		_, err := GetKey(MustUnwrap(v, 2))
		if err == nil {
			t.Errorf("expected err, got none")
		}
	})

	t.Run("set key twice", func(t *testing.T) {
		v := SetKey(Key(1), SetKey(Key(2), New(42)))
		got, _ := GetKey(v)
		want := Key(1)
		if diff := cmp.Diff(want, got); diff != "" {
			t.Errorf("unexpected result -want/+got:\n\t%s", diff)
		}
		_, err := GetKey(MustUnwrap(v, 1))
		if err == nil {
			t.Errorf("expected err, got none")
		}
	})
}

func TestSource(t *testing.T) {
	t.Run("outer source", func(t *testing.T) {
		v := SetSource(Source(0), SetKey(Key(1), New(42)))
		got, _ := GetSource(v)
		want := Source(0)
		if diff := cmp.Diff(want, got); diff != "" {
			t.Errorf("unexpected result -want/+got:\n\t%s", diff)
		}
		_, err := GetSource(MustUnwrap(v, 1))
		if err == nil {
			t.Errorf("expected err, got none")
		}

		v = SetSource(Source(1), v)
		got, _ = GetSource(v)
		want = Source(1)
		if diff := cmp.Diff(want, got); diff != "" {
			t.Errorf("unexpected result -want/+got:\n\t%s", diff)
		}
		_, err = GetSource(MustUnwrap(v, 2))
		if err == nil {
			t.Errorf("expected err, got none")
		}
	})

	t.Run("inner source", func(t *testing.T) {
		v := SetKey(Key(1), SetSource(Source(0), New(42)))
		got, _ := GetSource(v)
		want := Source(0)
		if diff := cmp.Diff(want, got); diff != "" {
			t.Errorf("unexpected result -want/+got:\n\t%s", diff)
		}

		v = SetSource(Source(1), v)
		got, _ = GetSource(v)
		want = Source(1)
		if diff := cmp.Diff(want, got); diff != "" {
			t.Errorf("unexpected result -want/+got:\n\t%s", diff)
		}
		_, err := GetSource(MustUnwrap(v, 2))
		if err == nil {
			t.Errorf("expected err, got none")
		}
	})

	t.Run("set source twice", func(t *testing.T) {
		v := SetSource(Source(1), SetSource(Source(2), New(42)))
		got, _ := GetSource(v)
		want := Source(1)
		if diff := cmp.Diff(want, got); diff != "" {
			t.Errorf("unexpected result -want/+got:\n\t%s", diff)
		}
		_, err := GetSource(MustUnwrap(v, 1))
		if err == nil {
			t.Errorf("expected err, got none")
		}
	})
}

func TestTime(t *testing.T) {
	t.Run("outer time", func(t *testing.T) {
		v := SetTime(Timestamp(0), Timestamp(10), SetKey(Key(1), SetSource(Source(0), New(42))))
		gotT, gotW, _ := GetTime(v)
		if gotT != Timestamp(0) || gotW != Timestamp(10) {
			t.Errorf("unexpected result")
		}
		_, _, err := GetTime(MustUnwrap(v, 1))
		if err == nil {
			t.Errorf("expected err, got none")
		}

		v = SetTime(Timestamp(1), Timestamp(11), v)
		gotT, gotW, _ = GetTime(v)
		if gotT != Timestamp(1) || gotW != Timestamp(11) {
			t.Errorf("unexpected result")
		}
		_, _, err = GetTime(MustUnwrap(v, 1))
		if err == nil {
			t.Errorf("expected err, got none")
		}
	})

	t.Run("inner time", func(t *testing.T) {
		v := SetSource(Source(0), SetTime(Timestamp(0), Timestamp(10), SetKey(Key(1), New(42))))
		gotT, gotW, _ := GetTime(v)
		if gotT != Timestamp(0) || gotW != Timestamp(10) {
			t.Errorf("unexpected result")
		}

		v = SetTime(Timestamp(1), Timestamp(11), v)
		gotT, gotW, _ = GetTime(v)
		if gotT != Timestamp(1) || gotW != Timestamp(11) {
			t.Errorf("unexpected result")
		}
		_, _, err := GetTime(MustUnwrap(v, 2))
		if err == nil {
			t.Errorf("expected err, got none")
		}
	})

	t.Run("set time twice", func(t *testing.T) {
		v := SetTime(Timestamp(0), Timestamp(10), SetTime(Timestamp(1), Timestamp(11), New(42)))
		gotT, gotW, _ := GetTime(v)
		if gotT != Timestamp(0) || gotW != Timestamp(10) {
			t.Errorf("unexpected result")
		}
		_, _, err := GetTime(MustUnwrap(v, 1))
		if err == nil {
			t.Errorf("expected err, got none")
		}
	})
}
