package ssp

import (
	"testing"

	"github.com/affo/ssp/values"
	"github.com/google/go-cmp/cmp"
)

type sliceCollector struct {
	vs []int
}

func (c *sliceCollector) Collect(v values.Value) {
	c.vs = append(c.vs, v.Int())
}

func checkWindows(t *testing.T, wm WindowManager, ts values.Timestamp, size int64, tss ...values.Timestamp) {
	t.Helper()

	starts := make(map[values.Timestamp]bool)
	for _, ts := range tss {
		starts[ts] = true
	}
	_ = wm.ForEachWindow(ts, func(w *Window) error {
		if _, ok := starts[w.Start()]; !ok {
			t.Errorf("unexpected start: %v", w.Start())
		}
		if int64(w.Stop()) != int64(w.Start())+size {
			t.Errorf("unexpected window size: [%v, %v)", w.Start(), w.Stop())
		}
		delete(starts, w.Start())
		return nil
	})
	if len(starts) > 0 {
		t.Errorf("non-empty starts: %v", starts)
	}
}

func TestFixedWindowManager(t *testing.T) {
	t.Run("slide 1", func(t *testing.T) {
		wm := NewFixedWindowManager(5, 1, values.New(0))

		// Given ts 0, I expect only 1 window.
		checkWindows(t, wm, 0, 5, 0)
		// Given ts 1, I expect 2.
		checkWindows(t, wm, 1, 5, 0, 1)
		// Given ts 10, I expect every window that contains 10.
		checkWindows(t, wm, 10, 5, 6, 7, 8, 9, 10)
		checkWindows(t, wm, 7, 5, 3, 4, 5, 6, 7)
	})

	t.Run("tumbling", func(t *testing.T) {
		wm := NewFixedWindowManager(3, 3, values.New(0))

		checkWindows(t, wm, 0, 3, 0)
		checkWindows(t, wm, 1, 3, 0)
		checkWindows(t, wm, 4, 3, 3)
		checkWindows(t, wm, 7, 3, 6)
	})

	t.Run("slide > size", func(t *testing.T) {
		wm := NewFixedWindowManager(5, 6, values.New(0))

		checkWindows(t, wm, 0, 5, 0)
		checkWindows(t, wm, 1, 5, 0)
		checkWindows(t, wm, 2, 5, 0)
		checkWindows(t, wm, 3, 5, 0)
		checkWindows(t, wm, 4, 5, 0)
		// 5 does not fall into any window in this case!
		checkWindows(t, wm, 5, 5)
		checkWindows(t, wm, 6, 5, 6)
		// 42 is 6 * 7 => the 7th window starts at 42
		checkWindows(t, wm, 42, 5, 42)
		checkWindows(t, wm, 43, 5, 42)
		checkWindows(t, wm, 46, 5, 42)
		checkWindows(t, wm, 50, 5, 48)
	})

	t.Run("fire", func(t *testing.T) {
		wm := NewFixedWindowManager(5, 6, values.New(0))

		add := func(w *Window) error {
			v := values.SetTime(w.Start(), w.Start(), values.NewNull(values.Int))
			w.AddElement(v.(values.TimestampedValue))
			return nil
		}
		_ = wm.ForEachWindow(0, add)  // window [0, 5)
		_ = wm.ForEachWindow(42, add) // window [42, 47)
		_ = wm.ForEachWindow(10, add) // window [6, 11)
		_ = wm.ForEachWindow(2, add)  // window [0, 5)
		_ = wm.ForEachWindow(3, add)  // window [0, 5)
		_ = wm.ForEachWindow(33, add) // window [30, 35)
		_ = wm.ForEachWindow(45, add) // window [42, 47)

		// Given watermark 10 should only close window [0, 5).
		_ = wm.ForEachClosedWindow(10, func(w *Window) error {
			if w.Start() != 0 || w.Stop() != 5 {
				t.Errorf("unexpected closed window: %v", w)
			}
			count := 0
			_ = w.Range(func(v values.TimestampedValue) error {
				count++
				return nil
			})
			if count != 3 {
				t.Errorf("unexpected number of elements: %v", count)
			}
			return nil
		})

		// Given a lower watermark should not cause anything.
		_ = wm.ForEachClosedWindow(7, func(w *Window) error {
			t.Error("this should not get invoked")
			return nil
		})

		// Watermark 46 should close windows [6, 11), [30, 35).
		_ = wm.ForEachClosedWindow(46, func(w *Window) error {
			if !((w.Start() == 6 && w.Stop() == 11) ||
				(w.Start() == 30 && w.Stop() == 35)) {
				t.Errorf("unexpected closed window: %v", w)
			}
			count := 0
			_ = w.Range(func(v values.TimestampedValue) error {
				count++
				return nil
			})
			if count != 1 {
				t.Errorf("unexpected number of elements: %v", count)
			}
			return nil
		})

		// Watermark 47 should close window [42, 47).
		_ = wm.ForEachClosedWindow(47, func(w *Window) error {
			if w.Start() != 42 || w.Stop() != 47 {
				t.Errorf("unexpected closed window: %v", w)
			}
			count := 0
			_ = w.Range(func(v values.TimestampedValue) error {
				count++
				return nil
			})
			if count != 2 {
				t.Errorf("unexpected number of elements: %v", count)
			}
			return nil
		})
	})

}

func TestWindowedNode(t *testing.T) {
	t.Run("update window state", func(t *testing.T) {
		n := NewWindowedNode(3, 3, values.New(0),
			func(w *Window, collector Collector, v values.TimestampedValue) error {
				w.State = values.New(w.State.Int() + v.Int())
				return nil
			},
			func(w *Window, collector Collector) error {
				collector.Collect(w.State)
				return nil
			},
		).SetName("windowed counter")

		c := &sliceCollector{}
		if err := n.Do(c, values.SetTime(1, 1, values.New(1))); err != nil {
			t.Errorf("unexpected error")
		}
		// No window closed.
		if len(c.vs) > 0 {
			t.Errorf("unexpected values collected: %v", c.vs)
		}
		// This should make it close.
		if err := n.Do(c, values.SetTime(1, 3, values.New(2))); err != nil {
			t.Errorf("unexpected error")
		}
		want := []int{3}
		if diff := cmp.Diff(want, c.vs); diff != "" {
			t.Errorf("unexpected values -want/+got:\n\t%s", diff)
		}

		// Now collect some values
		// [3, 6)
		if err := n.Do(c, values.SetTime(4, 0, values.New(4))); err != nil {
			t.Errorf("unexpected error")
		}
		if err := n.Do(c, values.SetTime(5, 0, values.New(5))); err != nil {
			t.Errorf("unexpected error")
		}
		if err := n.Do(c, values.SetTime(5, 0, values.New(5))); err != nil {
			t.Errorf("unexpected error")
		}
		// [6, 9)
		if err := n.Do(c, values.SetTime(6, 0, values.New(6))); err != nil {
			t.Errorf("unexpected error")
		}
		if err := n.Do(c, values.SetTime(8, 0, values.New(8))); err != nil {
			t.Errorf("unexpected error")
		}
		// [9, 12)
		if err := n.Do(c, values.SetTime(9, 0, values.New(9))); err != nil {
			t.Errorf("unexpected error")
		}
		if err := n.Do(c, values.SetTime(12, 12, values.New(12))); err != nil {
			t.Errorf("unexpected error")
		}
		want = append(want, 14, 14, 9)
		if diff := cmp.Diff(want, c.vs); diff != "" {
			t.Errorf("unexpected values -want/+got:\n\t%s", diff)
		}

		// Some out-of-orderness.
		// Produces a window with that unique value.
		if err := n.Do(c, values.SetTime(1, 0, values.New(12))); err != nil {
			t.Errorf("unexpected error")
		}
		want = append(want, 12)
		if diff := cmp.Diff(want, c.vs); diff != "" {
			t.Errorf("unexpected values -want/+got:\n\t%s", diff)
		}
		if err := n.Do(c, values.SetTime(11, 0, values.New(11))); err != nil {
			t.Errorf("unexpected error")
		}
		want = append(want, 11)
		if diff := cmp.Diff(want, c.vs); diff != "" {
			t.Errorf("unexpected values -want/+got:\n\t%s", diff)
		}
	})
}
