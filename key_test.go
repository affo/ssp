package ssp

import (
	"testing"

	"github.com/affo/ssp/values"
)

func TestStringValueKeySelector_GetKey(t *testing.T) {
	ks := NewStringValueKeySelector(func(v values.Value) string {
		return v.String()
	})

	k1 := ks.GetKey(values.New("foo"))
	k2 := ks.GetKey(values.New("bar"))
	if k1 == k2 {
		t.Error("key clash")
	}
	k3 := ks.GetKey(values.New("foo"))
	k4 := ks.GetKey(values.New("bar"))
	if k1 != k3 || k2 != k4 {
		t.Errorf("non-detrministic behavior detected")
	}
}
