package values

import "fmt"

var _ Value = (*meta)(nil)

const (
	_ Type = Unknown
	Close
)

type meta struct {
	t Type
}

// TODO(affo): find a better way for extending types for values.
//  Here, a meta could be of any type. We want it to be only some subset of types.
func NewMeta(t Type) Value {
	return meta{t: t}
}

func (m meta) Type() Type {
	return m.t
}

func (m meta) Get() interface{} {
	panic("cannot return interface{} from meta value")
}

func (m meta) IsNull() bool {
	return false
}

func (m meta) Unwrap() (Value, error) {
	return nil, fmt.Errorf("meta value cannot be unwrapped")
}

func (m meta) Clone() Value {
	return NewMeta(m.t)
}

func (m meta) Int() int {
	panic("cannot return primitive type from meta value")
}

func (m meta) Bool() bool {
	panic("cannot return primitive type from meta value")
}

func (m meta) Float32() float32 {
	panic("cannot return primitive type from meta value")
}

func (m meta) Float64() float64 {
	panic("cannot return primitive type from meta value")
}

func (m meta) Int16() int16 {
	panic("cannot return primitive type from meta value")
}

func (m meta) Int32() int32 {
	panic("cannot return primitive type from meta value")
}

func (m meta) Int64() int64 {
	panic("cannot return primitive type from meta value")
}

func (m meta) Int8() int8 {
	panic("cannot return primitive type from meta value")
}

func (m meta) String() string {
	return fmt.Sprintf("meta(%v)", m.t)
}

func (m meta) Uint16() uint16 {
	panic("cannot return primitive type from meta value")
}

func (m meta) Uint32() uint32 {
	panic("cannot return primitive type from meta value")
}

func (m meta) Uint64() uint64 {
	panic("cannot return primitive type from meta value")
}

func (m meta) Uint8() uint8 {
	panic("cannot return primitive type from meta value")
}
