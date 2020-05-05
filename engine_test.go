package ssp

import (
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/affo/ssp/values"
	"github.com/fortytw2/leaktest"
	"github.com/google/go-cmp/cmp"
)

func TestOperator(t *testing.T) {
	defer leaktest.Check(t)()

	n := NewNode(func(collector Collector, v values.Value) error {
		collector.Collect(values.New(strings.ToUpper(v.String())))
		return nil
	})
	in := NewInfiniteStream()
	out := NewInfiniteStream()
	// Need some buffering to avoid deadlock because we consume at the end.
	out.bufferSize = 10
	o := NewOperator(n)
	o.In(in)
	o.Out(out)
	o.Open()
	defer func() {
		if err := o.Close(); err != nil {
			t.Fatalf("unexpected error on close: %v", err)
		}
	}()

	// Must set a key, even if useless.
	in.Collect(values.SetKey(0, values.New("hello")))
	in.Collect(values.SetKey(0, values.New("this")))
	in.Collect(values.SetKey(0, values.New("is")))
	in.Collect(values.SetKey(0, values.New("ssp")))
	SendClose(in)

	want := []string{"HELLO", "THIS", "IS", "SSP"}
	for i := 0; i < len(want); i++ {
		if got := out.Next().String(); want[i] != got {
			t.Errorf("expected %s got %v", want[i], got)
		}
	}
}

func TestParallelOperator(t *testing.T) {
	defer leaktest.Check(t)()

	ks := NewStringValueKeySelector(func(v values.Value) string {
		return v.String()
	})
	in := NewInfiniteStream()
	out := NewInfiniteStream()
	// Need some buffering to avoid deadlock because we consume at the end.
	out.bufferSize = 10
	o := NewParallelOperator(4, func() *Operator {
		return NewOperator(NewStatefulNode(values.New(int64(0)),
			func(state values.Value, collector Collector, v values.Value) (values.Value, error) {
				count := state.Int64() + 1
				collector.Collect(values.New(fmt.Sprintf("%v: %d", v, count)))
				return values.New(count), nil
			}))
	}, WithInKeySelector(ks))
	o.In(in, func() Transport {
		return NewInfiniteStream()
	})
	o.Out([]Collector{out})
	o.Open()
	defer func() {
		SendClose(in)
		if err := o.Close(); err != nil {
			t.Fatalf("unexpected error on close: %v", err)
		}
	}()

	ins := []string{
		"hello",
		"this",
		"is",
		"ssp",
		"hello",
		"this",
		"is",
		"sparta",
		"sparta",
		"is",
		"leonida",
	}

	for _, i := range ins {
		in.Collect(values.New(i))
	}

	want := []string{
		"hello: 1",
		"hello: 2",
		"is: 1",
		"is: 2",
		"is: 3",
		"leonida: 1",
		"sparta: 1",
		"sparta: 2",
		"ssp: 1",
		"this: 1",
		"this: 2",
	}
	got := make([]string, len(ins))

	for i := 0; i < len(got); i++ {
		got[i] = out.Next().String()
	}
	sort.Strings(got)

	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("unexpected reesult:\n\t%s", diff)
	}
}

func TestEngine(t *testing.T) {
	defer leaktest.Check(t)()

	ctx := Context()
	p := NewNode(func(collector Collector, v values.Value) error {
		for i := 0; i < 5; i++ {
			collector.Collect(values.New(int64(i)))
		}
		return nil
	}).
		Out().
		Connect(ctx, NewStatefulNode(values.New(int64(0)),
			func(state values.Value, collector Collector, v values.Value) (updatedState values.Value, e error) {
				state = values.New(state.Int64() + v.Int64())
				collector.Collect(state)
				return state, e
			})).Out()

	sink, log := NewLogSink(values.Int64)
	p.Connect(ctx, sink)

	if err := Execute(ctx); err != nil {
		t.Fatal(err)
	}

	var got []int64
	for _, v := range log.GetValues() {
		got = append(got, v.Int64())
	}
	if diff := cmp.Diff([]int64{0, 1, 3, 6, 10}, got); diff != "" {
		t.Errorf("unexpected result -want/+got:\n\t%s", diff)
	}
}

func TestParallelEngine(t *testing.T) {
	defer leaktest.Check(t)()

	ctx := Context()
	p := NewNode(func(collector Collector, v values.Value) error {
		in := []string{
			"hello",
			"this",
			"is",
			"ssp",
			"hello",
			"this",
			"is",
			"sparta",
			"sparta",
			"is",
			"leonida",
		}
		for _, v := range in {
			collector.Collect(values.New(v))
		}
		return nil
	}).SetName("source").
		Out().
		KeyBy(NewStringValueKeySelector(func(v values.Value) string {
			return v.String()
		})).
		Connect(ctx, NewStatefulNode(values.New(int64(0)),
			func(state values.Value, collector Collector, v values.Value) (values.Value, error) {
				count := state.Int64() + 1
				collector.Collect(values.New(fmt.Sprintf("%v: %d", v, count)))
				return values.New(count), nil
			})).
		SetName("wordCounter").
		SetParallelism(4).
		Out()

	sink, log := NewLogSink(values.String)
	p.Connect(ctx, sink.SetName("sink"))

	if err := Execute(ctx); err != nil {
		t.Fatal(err)
	}

	want := []string{
		"hello: 1",
		"hello: 2",
		"is: 1",
		"is: 2",
		"is: 3",
		"leonida: 1",
		"sparta: 1",
		"sparta: 2",
		"ssp: 1",
		"this: 1",
		"this: 2",
	}
	var got []string
	for _, v := range log.GetValues() {
		got = append(got, v.String())
	}
	sort.Strings(got)
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("unexpected result -want/+got:\n\t%s", diff)
	}
}

func TestDataStreams(t *testing.T) {
	setup := func() (*dataStreams, int) {
		iss := make([]*infiniteStream, 10)
		for i := 0; i < len(iss); i++ {
			iss[i] = NewInfiniteStream()
		}
		return newDataStreams(iss...), len(iss)
	}

	t.Run("happy path", func(t *testing.T) {
		dss, ns := setup()

		for i := 0; i < ns; i++ {
			dss.ss[i].Collect(values.New(i))
		}

		for i := 0; i < ns; i++ {
			v := dss.Next()
			source, _ := values.GetSource(v)
			if val, source := v.Int(), int(source); val != source {
				t.Errorf("unexpected val/source: %d/%d", val, source)
			}
		}

		for i := 0; i < ns; i++ {
			SendClose(dss.ss[i])
		}
	})

	t.Run("partial close", func(t *testing.T) {
		dss, ns := setup()

		for i := 0; i < ns; i++ {
			dss.ss[i].Collect(values.New(i))
			SendClose(dss.ss[i])
		}

		for i := 0; i < ns; i++ {
			v := dss.Next()
			source, _ := values.GetSource(v)
			if val, source := v.Int(), int(source); val != source {
				t.Errorf("unexpected val/source: %d/%d", val, source)
			}
		}

		for i := 0; i < ns; i++ {
			v := dss.Next()
			if v != nil {
				t.Errorf("expected Next() to be nil, got %v instead", v)
			}
		}
	})
}

func TestWatermarker(t *testing.T) {
	setup := func(nSources int) (*watermarker, []*infiniteStream, func()) {
		iss := make([]*infiniteStream, nSources)
		for i := 0; i < len(iss); i++ {
			iss[i] = NewInfiniteStream()
		}
		return newWatermarker(newDataStreams(iss...), len(iss)), iss, func() {
			for i := 0; i < len(iss); i++ {
				SendClose(iss[i])
			}
		}
	}

	t.Run("single source", func(t *testing.T) {
		wmer, dss, closeFn := setup(1)
		defer closeFn()
		ds := dss[0]

		ds.Collect(values.SetTime(values.Timestamp(0), values.Timestamp(0), values.NewNull(values.Int)))
		v := wmer.Next()
		wantTs := values.Timestamp(0)
		wantWm := values.Timestamp(0)
		if ts, wm, err := values.GetTime(v); ts != wantTs || wm != wantWm || err != nil {
			t.Errorf("unexpected ts/wm: want: %d/%d, got: %d/%d, err: %v", wantTs, wantWm, ts, wm, err)
		}
		ds.Collect(values.SetTime(values.Timestamp(1), values.Timestamp(0), values.NewNull(values.Int)))
		v = wmer.Next()
		wantTs = values.Timestamp(1)
		wantWm = values.Timestamp(0)
		if ts, wm, err := values.GetTime(v); ts != wantTs || wm != wantWm || err != nil {
			t.Errorf("unexpected ts/wm: want: %d/%d, got: %d/%d, err: %v", wantTs, wantWm, ts, wm, err)
		}
		ds.Collect(values.SetTime(values.Timestamp(1), values.Timestamp(1), values.NewNull(values.Int)))
		v = wmer.Next()
		wantTs = values.Timestamp(1)
		wantWm = values.Timestamp(1)
		if ts, wm, err := values.GetTime(v); ts != wantTs || wm != wantWm || err != nil {
			t.Errorf("unexpected ts/wm: want: %d/%d, got: %d/%d, err: %v", wantTs, wantWm, ts, wm, err)
		}
		ds.Collect(values.SetTime(values.Timestamp(5), values.Timestamp(3), values.NewNull(values.Int)))
		v = wmer.Next()
		wantTs = values.Timestamp(5)
		wantWm = values.Timestamp(3)
		if ts, wm, err := values.GetTime(v); ts != wantTs || wm != wantWm || err != nil {
			t.Errorf("unexpected ts/wm: want: %d/%d, got: %d/%d, err: %v", wantTs, wantWm, ts, wm, err)
		}
		// Out order watermark.
		ds.Collect(values.SetTime(values.Timestamp(5), values.Timestamp(0), values.NewNull(values.Int)))
		v = wmer.Next()
		wantTs = values.Timestamp(5)
		wantWm = values.Timestamp(3)
		if ts, wm, err := values.GetTime(v); ts != wantTs || wm != wantWm || err != nil {
			t.Errorf("unexpected ts/wm: want: %d/%d, got: %d/%d, err: %v", wantTs, wantWm, ts, wm, err)
		}
		ds.Collect(values.SetTime(values.Timestamp(5), values.Timestamp(4), values.NewNull(values.Int)))
		v = wmer.Next()
		wantTs = values.Timestamp(5)
		wantWm = values.Timestamp(4)
		if ts, wm, err := values.GetTime(v); ts != wantTs || wm != wantWm || err != nil {
			t.Errorf("unexpected ts/wm: want: %d/%d, got: %d/%d, err: %v", wantTs, wantWm, ts, wm, err)
		}
	})

	t.Run("multi source", func(t *testing.T) {
		wmer, dss, closeFn := setup(3)
		defer closeFn()

		dss[0].Collect(values.SetTime(values.Timestamp(0), values.Timestamp(0), values.NewNull(values.Int)))
		v := wmer.Next()
		wantTs := values.Timestamp(0)
		wantWm := values.Timestamp(0)
		if ts, wm, err := values.GetTime(v); ts != wantTs || wm != wantWm || err != nil {
			t.Errorf("unexpected ts/wm: want: %d/%d, got: %d/%d, err: %v", wantTs, wantWm, ts, wm, err)
		}
		dss[1].Collect(values.SetTime(values.Timestamp(10), values.Timestamp(5), values.NewNull(values.Int)))
		v = wmer.Next()
		wantTs = values.Timestamp(10)
		wantWm = values.Timestamp(0)
		if ts, wm, err := values.GetTime(v); ts != wantTs || wm != wantWm || err != nil {
			t.Errorf("unexpected ts/wm: want: %d/%d, got: %d/%d, err: %v", wantTs, wantWm, ts, wm, err)
		}
		dss[2].Collect(values.SetTime(values.Timestamp(8), values.Timestamp(3), values.NewNull(values.Int)))
		v = wmer.Next()
		wantTs = values.Timestamp(8)
		wantWm = values.Timestamp(0)
		if ts, wm, err := values.GetTime(v); ts != wantTs || wm != wantWm || err != nil {
			t.Errorf("unexpected ts/wm: want: %d/%d, got: %d/%d, err: %v", wantTs, wantWm, ts, wm, err)
		}

		// The "blocking" source is the first one, now it increases the watermark.
		// We expect everyone else to update.
		dss[0].Collect(values.SetTime(values.Timestamp(11), values.Timestamp(10), values.NewNull(values.Int)))
		v = wmer.Next()
		wantTs = values.Timestamp(11)
		wantWm = values.Timestamp(3)
		if ts, wm, err := values.GetTime(v); ts != wantTs || wm != wantWm || err != nil {
			t.Errorf("unexpected ts/wm: want: %d/%d, got: %d/%d, err: %v", wantTs, wantWm, ts, wm, err)
		}

		// Out order watermark.
		dss[1].Collect(values.SetTime(values.Timestamp(5), values.Timestamp(0), values.NewNull(values.Int)))
		v = wmer.Next()
		wantTs = values.Timestamp(5)
		wantWm = values.Timestamp(3)
		if ts, wm, err := values.GetTime(v); ts != wantTs || wm != wantWm || err != nil {
			t.Errorf("unexpected ts/wm: want: %d/%d, got: %d/%d, err: %v", wantTs, wantWm, ts, wm, err)
		}

		// Now the "blocking" source is the last one.
		dss[2].Collect(values.SetTime(values.Timestamp(9), values.Timestamp(6), values.NewNull(values.Int)))
		v = wmer.Next()
		wantTs = values.Timestamp(9)
		wantWm = values.Timestamp(5)
		if ts, wm, err := values.GetTime(v); ts != wantTs || wm != wantWm || err != nil {
			t.Errorf("unexpected ts/wm: want: %d/%d, got: %d/%d, err: %v", wantTs, wantWm, ts, wm, err)
		}
	})
}

func TestSharedCollector(t *testing.T) {
	is := NewInfiniteStream()
	sc := newSharedCollector(is, 2)

	for i := 0; i < 100; i++ {
		sc.Collect(values.New(i))
		// Send a Close in the middle.
		// This should not cause any total closure.
		if i == 50 {
			SendClose(sc)
		}
	}

	for i := 0; i < 100; i++ {
		v := is.Next()
		if got, want := v.Int(), i; got != want {
			t.Errorf("unexpected value -want/+got:\n\t-\t%d\n\t+\t%d", want, got)
		}
	}

	// Can still produce/consume values normally.
	for i := 0; i < 100; i++ {
		sc.Collect(values.New(i))
	}
	for i := 0; i < 100; i++ {
		v := is.Next()
		if got, want := v.Int(), i; got != want {
			t.Errorf("unexpected value -want/+got:\n\t-\t%d\n\t+\t%d", want, got)
		}
	}

	// This should definitely close the stream.
	SendClose(sc)
	for i := 0; i < 100; i++ {
		sc.Collect(values.New(i))
	}

	for i := 0; i < 100; i++ {
		v := is.Next()
		if v != nil {
			t.Errorf("expected Next() to be nil, got %v instead", v)
		}
	}
}

func TestBroadcastCollector(t *testing.T) {
	iss := make([]Collector, 10)
	for i := 0; i < len(iss); i++ {
		iss[i] = NewInfiniteStream()
	}
	bc := newBroadCastCollector(iss)

	bc.Collect(values.New(42))
	for i := 0; i < len(iss); i++ {
		v := iss[i].(*infiniteStream).Next()
		if got, want := v.Int(), 42; got != want {
			t.Errorf("unexpected value -want/+got:\n\t-\t%d\n\t+\t%d", want, got)
		}
	}

	SendClose(bc)
	for i := 0; i < len(iss); i++ {
		v := iss[i].(*infiniteStream).Next()
		if v != nil {
			t.Errorf("expected Next() to be nil, got %v instead", v)
		}
	}
}

func TestPartitionedStream(t *testing.T) {
	defer leaktest.Check(t)()

	ds := NewStreamFromElements(NewIntValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)...)
	ks := FnKeySelector(func(v values.Value) values.Key {
		return values.Key(v.Int() % 2)
	})
	ps := NewPartitionedStream(2, ks, ds, func() Transport {
		return NewInfiniteStream()
	})

	dsp := ps.Stream(0)
	v := dsp.Next()
	if got, want := v.Int(), 2; got != want {
		t.Errorf("unexpected value -want/+got:\n\t-\t%d\n\t+\t%d", want, got)
	}
	v = dsp.Next()
	if got, want := v.Int(), 4; got != want {
		t.Errorf("unexpected value -want/+got:\n\t-\t%d\n\t+\t%d", want, got)
	}
	v = dsp.Next()
	if got, want := v.Int(), 6; got != want {
		t.Errorf("unexpected value -want/+got:\n\t-\t%d\n\t+\t%d", want, got)
	}
	v = dsp.Next()
	if got, want := v.Int(), 8; got != want {
		t.Errorf("unexpected value -want/+got:\n\t-\t%d\n\t+\t%d", want, got)
	}
	v = dsp.Next()
	if got, want := v.Int(), 10; got != want {
		t.Errorf("unexpected value -want/+got:\n\t-\t%d\n\t+\t%d", want, got)
	}
	v = dsp.Next()
	if v != nil {
		t.Errorf("expected Next() to be nil, got %v instead", v)
	}

	dsp = ps.Stream(1)
	v = dsp.Next()
	if got, want := v.Int(), 1; got != want {
		t.Errorf("unexpected value -want/+got:\n\t-\t%d\n\t+\t%d", want, got)
	}
	v = dsp.Next()
	if got, want := v.Int(), 3; got != want {
		t.Errorf("unexpected value -want/+got:\n\t-\t%d\n\t+\t%d", want, got)
	}
	v = dsp.Next()
	if got, want := v.Int(), 5; got != want {
		t.Errorf("unexpected value -want/+got:\n\t-\t%d\n\t+\t%d", want, got)
	}
	v = dsp.Next()
	if got, want := v.Int(), 7; got != want {
		t.Errorf("unexpected value -want/+got:\n\t-\t%d\n\t+\t%d", want, got)
	}
	v = dsp.Next()
	if got, want := v.Int(), 9; got != want {
		t.Errorf("unexpected value -want/+got:\n\t-\t%d\n\t+\t%d", want, got)
	}
	v = dsp.Next()
	if v != nil {
		t.Errorf("expected Next() to be nil, got %v instead", v)
	}
}

func TestParallelEngine_MultipleInputs(t *testing.T) {
	defer leaktest.Check(t)()

	ctx := Context()
	source := NewNode(func(collector Collector, v values.Value) error {
		in := []string{
			"hello",
			"this",
			"is",
			"ssp",
		}
		for _, v := range in {
			collector.Collect(values.New(v))
		}
		return nil
	}).SetName("source").Out()

	upper := source.
		Connect(ctx, NewNode(func(collector Collector, v values.Value) error {
			collector.Collect(values.New(strings.ToUpper(v.String())))
			return nil
		})).SetName("upper")

	count := source.
		Connect(ctx, NewNode(func(collector Collector, v values.Value) error {
			collector.Collect(values.New(len(v.String())))
			return nil
		})).SetName("count")

	type state struct {
		s1 []values.Value
		s2 []values.Value
	}
	align := NewStatefulNode(values.New(&state{}), func(sv values.Value, collector Collector, v values.Value) (values.Value, error) {
		s := sv.Get().(*state)
		source, err := values.GetSource(v)
		if err != nil {
			return nil, err
		}
		if source == 0 {
			if len(s.s2) > 0 {
				ov := s.s2[0]
				s.s2 = s.s2[1:]
				collector.Collect(values.New(fmt.Sprintf("%v: %v", v, ov)))
			} else {
				s.s1 = append(s.s1, v)
			}
		} else {
			if len(s.s1) > 0 {
				ov := s.s1[0]
				s.s1 = s.s1[1:]
				collector.Collect(values.New(fmt.Sprintf("%v: %v", ov, v)))
			} else {
				s.s2 = append(s.s2, v)
			}
		}
		return sv, nil
	}).SetName("aligner")

	upper.Out().Connect(ctx, align)
	aligned := count.Out().Connect(ctx, align).Out()

	sink, log := NewLogSink(values.String)
	aligned.Connect(ctx, sink.SetName("sink"))

	if err := Execute(ctx); err != nil {
		t.Fatal(err)
	}

	want := []string{
		"HELLO: 5",
		"THIS: 4",
		"IS: 2",
		"SSP: 3",
	}
	var got []string
	for _, v := range log.GetValues() {
		got = append(got, v.String())
	}
	sort.Strings(want)
	sort.Strings(got)
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("unexpected result -want/+got:\n\t%s", diff)
	}
}

func TestParallelEngine_Windows(t *testing.T) {
	defer leaktest.Check(t)()

	type record struct {
		ts    values.Timestamp
		value string
	}
	type wState struct {
		key      values.Key
		keyValue string
		count    int
	}

	sink, log := NewLogSink(values.String)
	ctx := Context()
	NewNode(func(collector Collector, _ values.Value) error {
		in := []record{
			{ts: 1, value: "foo"},
			{ts: 1, value: "foo"},
			{ts: 2, value: "foo"},
			{ts: 8, value: "foo"},
			{ts: 5, value: "foo"},
			{ts: 5, value: "buz"},
			{ts: 5, value: "buz"},
			{ts: 7, value: "buz"},
			{ts: 6, value: "buz"},
			{ts: 10, value: "foo"},
			{ts: 10, value: "bar"},
			{ts: 10, value: "buz"},
			{ts: 2, value: "foo"}, // out of order.
			{ts: 13, value: "bar"},
			{ts: 15, value: "buz"},
			{ts: 3, value: "buz"}, // out of order.
			{ts: 31, value: "foo"},
			{ts: 31, value: "bar"},
			{ts: 30, value: "foo"},
			{ts: 20, value: "bar"},  // out of order.
			{ts: 100, value: "foo"}, // trigger the rest.
			{ts: 100, value: "bar"}, // trigger the rest.
			{ts: 100, value: "buz"}, // trigger the rest.
		}
		for _, v := range in {
			collector.Collect(values.New(v))
		}
		return nil
	}).SetName("source").
		Out().
		Connect(ctx, AssignTimestamp(func(v values.Value) (ts values.Timestamp, wm values.Timestamp) {
			r := v.Get().(record)
			// Fixed delay.
			return r.ts, r.ts - 5
		})).SetName("timestampExtractor").
		Out().
		KeyBy(NewStringValueKeySelector(func(v values.Value) string {
			return v.Get().(record).value
		})).
		Connect(ctx, NewWindowedNode(
			5, 2, values.New(wState{}),
			func(w *Window, collector Collector, v values.TimestampedValue) error {
				s := w.State.Get().(wState)
				key, err := values.GetKey(v)
				if err != nil {
					return err
				}
				s.key = key
				s.keyValue = v.Get().(record).value
				s.count++
				w.State = values.New(s)
				return nil
			},
			func(w *Window, collector Collector) error {
				s := w.State.Get().(wState)
				collector.Collect(values.New(fmt.Sprintf("%v: %s - %d", w, s.keyValue, s.count)))
				return nil
			})).
		SetName("windowedWordCounter").
		SetParallelism(4).
		Out().
		Connect(ctx, sink.SetName("sink"))

	if err := Execute(ctx); err != nil {
		t.Fatal(err)
	}

	want := []string{
		// foo's ts: 1 1 2 5 8 10 (2) 30 31 100.
		"[0, 5): foo - 3",
		"[2, 7): foo - 3",
		"[0, 5): foo - 1", // caused by {ts: 2, value: "foo"}.
		"[4, 9): foo - 2",
		"[6, 11): foo - 2",
		"[8, 13): foo - 2",
		"[10, 15): foo - 1",
		"[26, 31): foo - 1",
		"[28, 33): foo - 2",
		"[30, 35): foo - 2",

		// bar's ts: 10 13 31 (20) 100.
		"[6, 11): bar - 1",
		"[8, 13): bar - 1",
		"[10, 15): bar - 2",
		"[12, 17): bar - 1",
		"[28, 33): bar - 1",
		"[30, 35): bar - 1",
		"[16, 21): bar - 1", // caused by {ts: 20, value: "bar"}.
		"[18, 23): bar - 1", // caused by {ts: 20, value: "bar"}.
		"[20, 25): bar - 1", // caused by {ts: 20, value: "bar"}.

		// buz's ts: 5 5 6 7 10 15 (3) 100.
		"[2, 7): buz - 3",
		"[4, 9): buz - 4",
		"[6, 11): buz - 3",
		"[8, 13): buz - 1",
		"[10, 15): buz - 1",
		"[12, 17): buz - 1",
		"[14, 19): buz - 1",
		"[0, 5): buz - 1", // caused by {ts: 3, value: "buz"}.
		"[2, 7): buz - 1", // caused by {ts: 3, value: "buz"}.
	}
	var got []string
	for _, v := range log.GetValues() {
		got = append(got, v.String())
	}
	sort.Strings(want)
	sort.Strings(got)
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("unexpected result -want/+got:\n\t%s", diff)
	}
}
