package values

type Source int

type ValueWithSource interface {
	Value
	Source() Source

	setSource(Source)
}

var _ ValueWithSource = (*valueWithSource)(nil)

type valueWithSource struct {
	s Source
	Value
}

func (v *valueWithSource) Source() Source {
	return v.s
}

func (v *valueWithSource) setSource(s Source) {
	v.s = s
}

func (v *valueWithSource) Unwrap() (Value, error) {
	return v.Value, nil
}

func (v *valueWithSource) Clone() Value {
	c := v.Value.Clone()
	return SetSource(v.s, c)
}

func SetSource(s Source, v Value) Value {
	ws, err := GetValueWithSource(v)
	if err != nil {
		return &valueWithSource{s: s, Value: v}
	}
	ws.setSource(s)
	return v
}

func GetValueWithSource(v Value) (ValueWithSource, error) {
	for {
		if ws, ok := v.(ValueWithSource); ok {
			return ws, nil
		}
		uv, err := v.Unwrap()
		if err != nil {
			return nil, err
		}
		v = uv
	}
}

func GetSource(v Value) (Source, error) {
	ws, err := GetValueWithSource(v)
	if err != nil {
		return 0, err
	}
	return ws.Source(), nil
}
