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
		ds := NewInfiniteStream(values.Int64, 1024)
		defer ds.Close()
	})

	t.Run("put values", func(t *testing.T) {
		size := 1024
		ds := NewInfiniteStream(values.Int64, size)
		defer ds.Close()

		for i := 0; i < size; i++ {
			ds.Collect(values.New(int64(i)))
		}

		for i := 0; i < size; i++ {
			v := ds.Next()
			if v.Int64() != int64(i) {
				t.Errorf("expected %d got %v", i, v)
			}
		}
	})

	t.Run("overcome size", func(t *testing.T) {
		defer leaktest.Check(t)()

		ds := NewInfiniteStream(values.Int64, 0)

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
		ds.Close()
		wg.Wait()

		if diff := cmp.Diff([]int64{42, 84}, got); diff != "" {
			t.Errorf("unexpected values -want/+got:\n\t%s", diff)
		}
	})
}
