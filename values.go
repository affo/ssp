package ssp

import (
	"fmt"
	"strconv"
)

type Type int

const (
	Int Type = iota
	Float
	Bool
	String
)

type Value interface {
	Type() Type
	Get() interface{}
	IsNull() bool
	String() string
}

func AssertType(v Value, t Type) {
	if v.Type() != t {
		panic(fmt.Errorf("value %v:%v is not of type %v", v, v.Type(), t))
	}
}

func GetInt(v Value) int64 {
	AssertType(v, Int)
	return v.Get().(int64)
}

func GetFloat(v Value) float64 {
	AssertType(v, Float)
	return v.Get().(float64)
}

func GetBool(v Value) bool {
	AssertType(v, Bool)
	return v.Get().(bool)
}

func GetString(v Value) string {
	AssertType(v, String)
	return v.Get().(string)
}

type nullValue struct {
	t Type
}

func (v nullValue) Type() Type {
	return v.t
}

func (v nullValue) Get() interface{} {
	return nil
}

func (v nullValue) IsNull() bool {
	return true
}

func (v nullValue) String() string {
	return "nil"
}

type intValue struct {
	v int64
}

func (v intValue) Type() Type {
	return Int
}

func (v intValue) Get() interface{} {
	return v.v
}

func (v intValue) IsNull() bool {
	return false
}

func (v intValue) String() string {
	return strconv.FormatInt(v.v, 10)
}

// TODO(affo): implement the other values.

func NewValue(v interface{}) Value {
	if v == nil {
		panic(fmt.Errorf("cannot create value from nil"))
	}
	switch v := v.(type) {
	case int:
		return intValue{v: int64(v)}
	case int8:
		return intValue{v: int64(v)}
	case int16:
		return intValue{v: int64(v)}
	case int32:
		return intValue{v: int64(v)}
	case int64:
		return intValue{v: v}
	default:
		panic(fmt.Errorf("cannot create value from type %T", v))
	}
}

func NewNull(t Type) Value {
	return nullValue{t: t}
}
