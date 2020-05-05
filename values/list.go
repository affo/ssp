package values

import "fmt"

var _ Value = (*List)(nil)

type List struct {
	t  Type
	vs []Value
}

func NewList(t Type) *List {
	return &List{t: t}
}

// TODO(affo): find a better way of expressing types.
//  A list if of type List<T>...
func (l *List) Type() Type {
	return l.t
}

func (l *List) Get() interface{} {
	return l.GetValues()
}

func (l *List) GetValues() []Value {
	return l.vs
}

func (l *List) AddValue(v Value) error {
	if v.Type() != l.t {
		return fmt.Errorf("unexpected type, want %v, got %v", l.t, v.Type())
	}
	l.vs = append(l.vs, v)
	return nil
}

func (l *List) Clone() Value {
	cl := NewList(l.t)
	for _, v := range l.vs {
		_ = cl.AddValue(v)
	}
	return cl
}

func (l *List) IsNull() bool {
	return false
}

func (l *List) Unwrap() (Value, error) {
	return nil, fmt.Errorf("list cannot be unwrapped")
}

func (l *List) Int() int {
	panic("cannot return primitive type from list")
}

func (l *List) Bool() bool {
	panic("cannot return primitive type from list")
}

func (l *List) Float32() float32 {
	panic("cannot return primitive type from list")
}

func (l *List) Float64() float64 {
	panic("cannot return primitive type from list")
}

func (l *List) Int16() int16 {
	panic("cannot return primitive type from list")
}

func (l *List) Int32() int32 {
	panic("cannot return primitive type from list")
}

func (l *List) Int64() int64 {
	panic("cannot return primitive type from list")
}

func (l *List) Int8() int8 {
	panic("cannot return primitive type from list")
}

func (l *List) String() string {
	return fmt.Sprintf("%v", l.vs)
}

func (l *List) Uint16() uint16 {
	panic("cannot return primitive type from list")
}

func (l *List) Uint32() uint32 {
	panic("cannot return primitive type from list")
}

func (l *List) Uint64() uint64 {
	panic("cannot return primitive type from list")
}

func (l *List) Uint8() uint8 {
	panic("cannot return primitive type from list")
}
