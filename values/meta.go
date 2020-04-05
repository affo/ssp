package values

import "fmt"

var _ Value = (*meta)(nil)

const (
	_ Type = iota
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
	panic("implement me")
}

func (m meta) IsNull() bool {
	panic("implement me")
}

func (m meta) Unwrap() Value {
	panic("list cannot be unwrapped")
}

func (m meta) Int() int {
	panic("implement me")
}

func (m meta) Bool() bool {
	panic("implement me")
}

func (m meta) Float32() float32 {
	panic("implement me")
}

func (m meta) Float64() float64 {
	panic("implement me")
}

func (m meta) Int16() int16 {
	panic("implement me")
}

func (m meta) Int32() int32 {
	panic("implement me")
}

func (m meta) Int64() int64 {
	panic("implement me")
}

func (m meta) Int8() int8 {
	panic("implement me")
}

func (m meta) String() string {
	return fmt.Sprintf("meta(%v)", m.t)
}

func (m meta) Uint16() uint16 {
	panic("implement me")
}

func (m meta) Uint32() uint32 {
	panic("implement me")
}

func (m meta) Uint64() uint64 {
	panic("implement me")
}

func (m meta) Uint8() uint8 {
	panic("implement me")
}
