package ssp

type DataStream interface {
	More() bool
	Next() Value
}
