[![Actions Status](https://github.com/affo/ssp/workflows/test/badge.svg)](https://github.com/affo/ssp/actions)

# SSP (Simple Stream Processor)

__TODO__

 - [x] graph builder lib
 - [x] walk graph
 - [x] abstractions on top
 - [x] serious engine
   - [x] parallel operators
   - [x] partitioned streams
   - [x] remove type checks and input stream check
   - [x] fix API to be cleaner
   - [x] distinguish records from different streams
   - [x] deeper testing
   - [x] word count benchmark
 - [x] manage time
   - [x] add timestamps to records
   - [x] watermarks
   - [x] windows
 - [ ] abstractions on top
 - [ ] add some simple planning

__Known Issues__

 - __Watermarks__ are _global_, but windows close on a _per-key_ basis.  
   This means that any value for any key/source sets a global watermark in an operator, but propagates only once records enter nodes!  
   This means that we have some difficulty in understanding what happens, especially for out-of-order values.  
   For example:
   
   ```
   Window: size 5, slide 2.
   Watermark: fixed offset 5.
   
   Windows: [0,  5), [2, 7), [4, 9), [6, 11), [8, 13), [10, 15), [12, 17), ...
   
   Records:
   {ts: 2, value: "buz"}
   {ts: 13, value: "bar"}
   {ts: 3, value: "buz"}
   {ts: 10, value: "buz"}
   
   Output: count of values per window.
   ```
   
   Record `13` will make the watermark for the `WindowNode` advance to `8` and, in theory, close `[0,  5), [2, 7)`.
   The watermark will advance both for `bar` and `buz`, but it will propagate to `buz` only when `3` gets processed.  
   Thus, the result will be:
   
   ```
   [0, 5) - buz: 2
   [2, 7) - buz: 2
   ...
   ```
   
   If the input, instead, is:
   
   ```
   {ts: 2, value: "buz"}
   {ts: 13, value: "bar"}
   {ts: 10, value: "buz"}
   {ts: 3, value: "buz"}
   ```
   
   So, the output will be:
   
   ```
   [0, 5) - buz: 1
   [0, 5) - buz: 1
   [2, 7) - buz: 1
   ...
   ```
   
   Because `10` will make `buz` aware of the `8` watermark and close `[0, 5)` without adding `3`.
   
   The goal is to make the two outputs be consistent.
   
 - `FixedWindowManager` stores windows in a `map`.  
   This makes iteration non-deterministic and makes some tests flaky.
   For example, we cannot determine the order in which windows close (the close function gets called).
 
__Optional__

 - [ ] generate graph as command?
 - [ ] multiple outputs for nodes (with tags?)
 - [ ] custom triggers (time)
 
## Code Examples

_NOTE_: These examples show some bits of code, but they could not reflect the exact status of `master`.

__Word Count__

```go
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
    panic(err)
}

fmt.Println(log.GetValues())
```

__Align__

```go
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
    source := values.GetSource(v)
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
    panic(err)
}

fmt.Println(log.GetValues())
```