package ssp

import (
	"fmt"
	"github.com/affo/ssp/values"
)

type Window struct {
	start values.Timestamp
	stop  values.Timestamp

	State    values.Value
	elements []values.TimestampedValue
}

func NewWindow(start values.Timestamp, stop values.Timestamp, state values.Value) *Window {
	return &Window{
		start:    start,
		stop:     stop,
		State:    state,
		elements: make([]values.TimestampedValue, 0),
	}
}

func (w *Window) Start() values.Timestamp {
	return w.start
}

func (w *Window) Stop() values.Timestamp {
	return w.stop
}

func (w *Window) AddElement(v values.TimestampedValue) {
	if v.Timestamp() < w.Start() || v.Timestamp() >= w.Stop() {
		panic("this should never happen")
	}
	w.elements = append(w.elements, v)
}

func (w *Window) Range(f func(v values.TimestampedValue) error) error {
	for _, e := range w.elements {
		if err := f(e); err != nil {
			return err
		}
	}
	return nil
}

func (w *Window) IsEmpty() bool {
	return len(w.elements) == 0
}

func (w *Window) String() string {
	return fmt.Sprintf("[%d, %d)", w.Start(), w.Stop())
}

// WindowManager provides active windows for a given time instant, and closes windows as time progresses.
type WindowManager interface {
	ForEachWindow(ts values.Timestamp, f func(w *Window) error) error
	ForEachClosedWindow(wm values.Timestamp, f func(w *Window) error) error
}

type FixedWindowManager struct {
	size  int64
	slide int64
	ws    map[values.Timestamp]*Window
	wss   []*Window
	state values.Value
	wm    values.Timestamp
}

func NewFixedWindowManager(size int, slide int, state values.Value) *FixedWindowManager {
	return &FixedWindowManager{
		size:  int64(size),
		slide: int64(slide),
		ws:    make(map[values.Timestamp]*Window),
		wss:   make([]*Window, 0),
		state: state,
		wm:    values.Timestamp(-1),
	}
}

func (m *FixedWindowManager) ForEachWindow(ts values.Timestamp, f func(w *Window) error) error {
	// Create windows if needed.
	// Note that if an element is out of order, we will open ad-hoc windows and close them on ForEachClosedWindow.
	start := m.slide * (int64(ts) / m.slide)
	// TODO: I think this could be translated somehow in the formula above.
	for start+m.size > int64(ts) {
		start -= m.slide
	}
	start += m.slide
	if start < 0 {
		start = 0
	}
	for ; start <= int64(ts); start += m.slide {
		startTs := values.Timestamp(start)
		if _, ok := m.ws[startTs]; !ok {
			m.ws[startTs] = NewWindow(startTs, values.Timestamp(start+m.size), m.state.Clone())
		}
	}

	// TODO: this should be more efficient. Consider using a BST, for example.
	for _, w := range m.ws {
		if ts >= w.Start() && ts < w.Stop() {
			if err := f(w); err != nil {
				return err
			}
		}
	}
	return nil
}

func (m *FixedWindowManager) ForEachClosedWindow(wm values.Timestamp, f func(w *Window) error) error {
	// Update the watermark.
	if wm > m.wm {
		m.wm = wm
	}
	// TODO: this should be more efficient. Consider using a BST, for example.
	// TODO: this is also not deterministic in order!
	for t, w := range m.ws {
		// This window can be closed.
		if m.wm >= w.Stop() {
			delete(m.ws, t)
			if err := f(w); err != nil {
				return err
			}
		}
	}
	return nil
}

type WindowFn func(w *Window, collector Collector, v values.TimestampedValue) error
type WindowCloseFn func(w *Window, collector Collector) error

type windowedNode struct {
	baseNode
	wm      WindowManager
	fn      WindowFn
	closeFn WindowCloseFn

	// For cloning.
	size  int
	slide int
	state values.Value
}

func NewWindowedNode(size, slide int, state values.Value, fn WindowFn, closeFn WindowCloseFn) Node {
	if size <= 0 || slide <= 0 {
		panic("size and slide must be greater than 0")
	}
	return &windowedNode{
		baseNode: newBaseNode(),
		wm:       NewFixedWindowManager(size, slide, state),
		size:     size,
		slide:    slide,
		state:    state,
		fn:       fn,
		closeFn:  closeFn,
	}
}

func (n *windowedNode) Do(collector Collector, v values.Value) error {
	tsv, err := values.GetTimestampedValue(v)
	if err != nil {
		return fmt.Errorf("values entering a window should be timestamped, this is not: %v", err)
	}

	if err := n.wm.ForEachWindow(tsv.Timestamp(), func(w *Window) error {
		return n.fn(w, collector, tsv)
	}); err != nil {
		return err
	}

	return n.wm.ForEachClosedWindow(tsv.Watermark(), func(w *Window) error {
		return n.closeFn(w, collector)
	})
}

func (n *windowedNode) Out() *Arch {
	return NewLink(n)
}

func (n *windowedNode) SetParallelism(par int) Node {
	n.par = par
	return n
}

func (n *windowedNode) SetName(name string) Node {
	n.name = name
	return n
}

func (n *windowedNode) Clone() Node {
	return &windowedNode{
		baseNode: n.baseNode.Clone(),
		wm:       NewFixedWindowManager(n.size, n.slide, n.state),
		fn:       n.fn,
		closeFn:  n.closeFn,
		size:     n.size,
		slide:    n.slide,
		state:    n.state,
	}
}
