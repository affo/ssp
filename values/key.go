package values

type Key uint64

type KeyedValue interface {
	Value
	Key() Key

	setKey(Key)
}

var _ KeyedValue = (*keyedValue)(nil)

type keyedValue struct {
	k Key
	Value
}

func (v *keyedValue) Key() Key {
	return v.k
}

func (v *keyedValue) setKey(k Key) {
	v.k = k
}

func (v *keyedValue) Unwrap() (Value, error) {
	return v.Value, nil
}

func (v *keyedValue) Clone() Value {
	c := v.Value.Clone()
	return SetKey(v.k, c)
}

func SetKey(k Key, v Value) Value {
	kv, err := GetKeyedValue(v)
	if err != nil {
		return &keyedValue{k: k, Value: v}
	}
	kv.setKey(k)
	return v
}

func GetKeyedValue(v Value) (KeyedValue, error) {
	for {
		if kv, ok := v.(KeyedValue); ok {
			return kv, nil
		}
		uv, err := v.Unwrap()
		if err != nil {
			return nil, err
		}
		v = uv
	}
}

func GetKey(v Value) (Key, error) {
	kv, err := GetKeyedValue(v)
	if err != nil {
		return 0, err
	}
	return kv.Key(), nil
}
