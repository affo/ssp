//go:generate go run github.com/benbjohnson/tmpl -data=@topology.tmpldata -o topology.gen.go ./graph/graph.go.tmpl

package ssp

type Steer interface {
	Assign(v Value, buckets []interface{}) (bucket int)
}

type fixedSteer struct{}

func (r fixedSteer) Assign(v Value, buckets []interface{}) (bucket int) {
	return 0
}

func (r fixedSteer) String() string {
	return "fixed"
}

func FixedSteer() Steer {
	return fixedSteer{}
}
