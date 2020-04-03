package ssp

import (
	"sync"
	"testing"

	"github.com/affo/ssp/values"
	"github.com/fortytw2/leaktest"
	"github.com/google/go-cmp/cmp"
)

func TestInfiniteStream(t *testing.T) {
	t.Run("new", func(t *testing.T) {
		ds := NewInfiniteStream()
		defer SendClose(ds)
	})

	t.Run("put values", func(t *testing.T) {
		ds := NewInfiniteStream()
		ds.bufferSize = 1000
		defer SendClose(ds)

		for i := 0; i < ds.bufferSize; i++ {
			ds.Collect(values.New(int64(i)))
		}

		for i := 0; i < ds.bufferSize; i++ {
			v := ds.Next()
			if v.Int64() != int64(i) {
				t.Errorf("expected %d got %v", i, v)
			}
		}
	})

	t.Run("overcome size", func(t *testing.T) {
		defer leaktest.Check(t)()

		ds := NewInfiniteStream()
		ds.bufferSize = 0

		wg := sync.WaitGroup{}
		wg.Add(1)
		var got []int64
		go func() {
			for next := ds.Next(); next != nil; next = ds.Next() {
				got = append(got, next.Int64())
			}
			wg.Done()
		}()

		ds.Collect(values.New(int64(42)))
		ds.Collect(values.New(int64(84)))
		SendClose(ds)
		wg.Wait()

		if diff := cmp.Diff([]int64{42, 84}, got); diff != "" {
			t.Errorf("unexpected values -want/+got:\n\t%s", diff)
		}
	})
}
