package ssp

import (
	"hash/fnv"

	"github.com/affo/ssp/values"
)

type KeySelector interface {
	GetKey(v values.Value) values.Key
}

type fixedKeySelector struct{}

func NewFixedKeySelector() KeySelector {
	return fixedKeySelector{}
}

func (s fixedKeySelector) GetKey(v values.Value) values.Key {
	return 0
}

func (s fixedKeySelector) String() string {
	return "fixed"
}

type FnKeySelector func(v values.Value) values.Key

func (s FnKeySelector) GetKey(v values.Value) values.Key {
	return s(v)
}

type roundRobinKeySelector struct {
	n int
	i int
}

func NewRoundRobinKeySelector(n int) KeySelector {
	return &roundRobinKeySelector{
		n: n,
	}
}

func (s *roundRobinKeySelector) GetKey(v values.Value) values.Key {
	k := values.Key(s.i)
	s.i++
	if s.i == s.n {
		s.i = 0
	}
	return k
}

func (s *roundRobinKeySelector) String() string {
	return "roundRobin"
}

type stringValueKeySelector struct {
	f func(v values.Value) string
}

func NewStringValueKeySelector(f func(v values.Value) string) KeySelector {
	return &stringValueKeySelector{f: f}
}

func (s *stringValueKeySelector) GetKey(v values.Value) values.Key {
	st := s.f(v)
	h := fnv.New64a()
	_, _ = h.Write([]byte(st))
	return values.Key(h.Sum64())
}

func (s *stringValueKeySelector) String() string {
	return "stringValue"
}
