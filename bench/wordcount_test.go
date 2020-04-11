package bench

import (
	"fmt"
	"io/ioutil"
	"testing"

	"github.com/affo/ssp"
	"github.com/affo/ssp/values"
)

const bytesIn = 10 * 1024 * 1024 // 10MB

func BenchmarkWordCount(b *testing.B) {
	ctx := ssp.Context()
	source := ssp.NewNode(func(collector ssp.Collector, _ values.Value) error {
		ws, _ := getWords(bytesIn)
		for _, w := range ws {
			collector.Collect(values.New(w))
		}
		return nil
	}).SetName("words")

	source.Out().
		KeyBy(ssp.NewStringValueKeySelector(
			func(v values.Value) string {
				return v.String()
			})).
		Connect(ctx, ssp.NewStatefulNode(values.New(0),
			func(state values.Value, collector ssp.Collector, v values.Value) (value values.Value, e error) {
				c := state.Int()
				c++
				collector.Collect(values.New(fmt.Sprintf("%s: %d", v.String(), c)))
				return values.New(c), e
			})).
		SetName("counter").
		SetParallelism(12).Out().
		Connect(ctx, ssp.NewNode(func(_ ssp.Collector, v values.Value) error {
			_, err := fmt.Fprint(ioutil.Discard, v)
			return err
		}))

	for i := 0; i < b.N; i++ {
		b.ReportAllocs()
		if err := ssp.Execute(ctx); err != nil {
			b.Fatal(err)
		}
	}
}
