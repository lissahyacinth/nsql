package parser

import (
	"stream_combination/models"
	"stream_combination/processor"
)

type Value interface {
	Eq(other Value) BooleanValue
	NEq(other Value) BooleanValue
	Lt(other Value) BooleanValue
	Lte(other Value) BooleanValue
	Gt(other Value) BooleanValue
	Gte(other Value) BooleanValue
}

type Evaluatable interface {
	Node
	Compile(ctx *processor.ProcessorBuilder) func(models.EventLike) Value
}

type LT struct {
	LHS Evaluatable
	RHS Evaluatable
}

type LTE struct {
	LHS Evaluatable
	RHS Evaluatable
}

type GT struct {
	LHS Evaluatable
	RHS Evaluatable
}

type GTE struct {
	LHS Evaluatable
	RHS Evaluatable
}

type And struct {
	LHS Evaluatable
	RHS Evaluatable
}

type Or struct {
	LHS Evaluatable
	RHS Evaluatable
}

type EQ struct {
	LHS Evaluatable
	RHS Evaluatable
}

func (E EQ) Visit(ctx *processor.ProcessorBuilder) interface{} {
	return E.Compile(ctx)
}

func (E EQ) Compile(ctx *processor.ProcessorBuilder) func(models.EventLike) Value {
	leftFn := E.LHS.Compile(ctx)
	rightFn := E.RHS.Compile(ctx)
	return func(event models.EventLike) Value {
		return leftFn(event).Eq(rightFn(event))
	}
}

func (O Or) Visit(ctx *processor.ProcessorBuilder) interface{} {
	return O.Compile(ctx)
}

func (O Or) Compile(ctx *processor.ProcessorBuilder) func(models.EventLike) Value {
	leftFn := O.LHS.Compile(ctx)
	rightFn := O.RHS.Compile(ctx)
	return func(event models.EventLike) Value {
		return leftFn(event).(BooleanValue).Or(rightFn(event).(BooleanValue))
	}
}

type Negate struct {
	Inner Evaluatable
}

func (N Negate) Visit(ctx *processor.ProcessorBuilder) interface{} {
	// TODO
	panic("Implement me!")
}
