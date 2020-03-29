package ssp

import "github.com/affo/ssp/values"

type Steer interface {
	Assign(v values.Value) (bucket int)
}

type fixedSteer struct{}

func FixedSteer() Steer {
	return fixedSteer{}
}

func (f fixedSteer) Assign(v values.Value) (bucket int) {
	return 0
}

func (f fixedSteer) String() string {
	return "fixed"
}

type FnSteer func(v values.Value) int

func (s FnSteer) Assign(v values.Value) (bucket int) {
	return s(v)
}
