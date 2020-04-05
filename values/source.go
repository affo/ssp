package values

type Source int

type ValueWithSource interface {
	Value
	Source() Source
}

type valueWithSource struct {
	s Source
	Value
}

func NewValueWithSource(s Source, v Value) ValueWithSource {
	return valueWithSource{s: s, Value: v}
}

func (v valueWithSource) Source() Source {
	return v.s
}

func (v valueWithSource) Unwrap() Value {
	return v.Value
}

func GetSource(v Value) Source {
	for {
		if ws, ok := v.(ValueWithSource); ok {
			return ws.Source()
		}
		v = v.Unwrap()
	}
}
