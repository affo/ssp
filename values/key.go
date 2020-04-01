package values

type Key uint64

type KeyedValue interface {
	Value
	Unwrap() Value
	Key() Key
}

type keyedValue struct {
	k Key
	Value
}

func NewKeyedValue(k Key, v Value) KeyedValue {
	return keyedValue{k: k, Value: v}
}

func (v keyedValue) Key() Key {
	return v.k
}

func (v keyedValue) Unwrap() Value {
	return v.Value
}
