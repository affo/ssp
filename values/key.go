package values

type KeyedValue interface {
	Value
	Key() int
}

type keyedValue struct {
	k int
	Value
}

func NewKeyedValue(k int, v Value) KeyedValue {
	return keyedValue{k: k, Value: v}
}

func (v keyedValue) Key() int {
	return v.k
}
