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
