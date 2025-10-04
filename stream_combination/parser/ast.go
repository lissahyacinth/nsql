package parser

import (
	"fmt"
	"stream_combination/models"
	"stream_combination/processor"
	"time"
)

// This controls items being added to a ProcessorBuilder in the second stage of building.

type Node interface {
	Visit(ctx *processor.ProcessorBuilder) interface{}
}

type Source struct {
	StreamName string
	Alias      *string
}

func (S Source) Visit(ctx *processor.ProcessorBuilder) interface{} {
	sourceProcessor, _ := processor.NewSubjectReader(ctx.JetStream, S.StreamName)
	ctx.AddProcessor(sourceProcessor.ID(), sourceProcessor)
	if S.Alias != nil {
		ctx.AddAlias(*S.Alias, sourceProcessor.ID())
	} else {
		// If not aliased, use the source name
		ctx.AddAlias(S.StreamName, sourceProcessor.ID())
	}
	return sourceProcessor
}

type FieldReference struct {
	Source *string
	Field  string
}

func (F FieldReference) Visit(ctx *processor.ProcessorBuilder) interface{} {
	return F
}

func (F FieldReference) Compile(ctx *processor.ProcessorBuilder) func(models.EventLike) Value {
	var FieldValue string
	if F.Source != nil {
		FieldValue = fmt.Sprintf("%s.%s", *F.Source, F.Field)
	} else {
		FieldValue = F.Field
	}

	return func(event models.EventLike) Value {
		value := event.GetField(FieldValue)
		if value == nil {
			panic("invalid field value " + FieldValue)
		}
		return NewValue(value)
	}
}

type Column struct {
	Source *string
	Field  string
	Alias  *string
}

type SelectNode struct {
	Source Node
	Fields []Column
}

func (sel SelectNode) Visit(ctx *processor.ProcessorBuilder) interface{} {
	// TODO: Ensure Source adds itself to ctx.
	sourceProcessor := sel.Source.Visit(ctx).(processor.Processor)
	// TODO: Validate that the fields are valid from these sources, or that these sources indicate their provenance.
	fieldNames := make([]string, len(sel.Fields))
	for _, field := range sel.Fields {
		if field.Source != nil {
			fieldNames = append(fieldNames, fmt.Sprintf("%s.%s", field.Source, field.Field))
		} else {
			fieldNames = append(fieldNames, field.Field)
		}
	}
	filterProcessor, _ := processor.NewColumnFilter(fieldNames, 50)
	ctx.AddProcessor(filterProcessor.ID(), filterProcessor, sourceProcessor.ID())
	// Add Sink, even if it's the wrong place
	sinkProcessor := processor.NewConsoleSink()
	ctx.AddProcessor(sinkProcessor.ID(), &sinkProcessor, filterProcessor.ID())
	return sinkProcessor
}

type WhereNode struct {
	Source Node
	Filter Evaluatable
}

func toBoolFunc(valueFn func(models.EventLike) Value) func(models.EventLike) bool {
	return func(event models.EventLike) bool {
		val := valueFn(event)
		boolVal, ok := val.(BooleanValue)
		if !ok {
			panic(fmt.Sprintf("expected BooleanValue, got %T", val))
		}
		return boolVal.Unwrap()
	}
}

func (w WhereNode) Visit(ctx *processor.ProcessorBuilder) interface{} {
	// Need to provide a WhereProcessor
	sourceProcessor := w.Source.Visit(ctx).(processor.Processor)
	evaluationFn := toBoolFunc(w.Filter.Compile(ctx))
	// TODO: Make buffer size less arbitrary
	whereFilterProcessor, _ := processor.NewWhereFilter(evaluationFn, 50)
	ctx.AddProcessor(whereFilterProcessor.ID(), whereFilterProcessor, sourceProcessor.ID())
	return whereFilterProcessor
}

type Constant struct {
	value Value
}

func (C Constant) Visit(ctx *processor.ProcessorBuilder) interface{} {
	return C
}

func (C Constant) Compile(ctx *processor.ProcessorBuilder) func(models.EventLike) Value {
	return func(models.EventLike) Value {
		return C.value
	}
}

type JoinWindow struct {
	LHS    Node
	RHS    Node
	Within time.Duration
	On     Evaluatable
}

func (J JoinWindow) Visit(ctx *processor.ProcessorBuilder) interface{} {
	lhsSource := J.LHS.Visit(ctx).(processor.MessageProcessor)
	rhsSource := J.RHS.Visit(ctx).(processor.MessageProcessor)

	// TODO: Write Predicate function here.
	swj := processor.NewSlidingWindowJoin(J.Within, make([]processor.EquiJoinPredicate, 0))
	ctx.AddDualProcessor(swj.ID(), swj, lhsSource.ID(), rhsSource.ID())
	return swj
}
