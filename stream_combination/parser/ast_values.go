package parser

import "C"
import (
	"fmt"
	"strconv"
	"strings"
)

func NewValueInferenceFromString(value string) Value {
	if !strings.Contains(value, ".") && !strings.ContainsAny(value, "eE") {
		if val, err := strconv.ParseInt(value, 10, 64); err == nil {
			return IntValue{val: val}
		}
	}
	if val, err := strconv.ParseFloat(value, 64); err == nil {
		return FloatValue{val: val}
	}
	if val, err := strconv.ParseBool(value); err == nil {
		return BooleanValue{val: val}
	}
	return StringValue{value}
}

func NewValue(value interface{}) Value {
	if value == nil {
		return NullValue{}
	}
	switch value.(type) {
	case string:
		return NewValueInferenceFromString(value.(string))
	case float64:
		return FloatValue{val: value.(float64)}
	case int:
		return IntValue{val: value.(int64)}
	case bool:
		return BooleanValue{val: value.(bool)}
	default:
		panic(fmt.Sprintf("unknown type %T with value %#v", value, value))
	}
}

type BooleanValue struct{ val bool }

func (B BooleanValue) Unwrap() bool {
	return B.val
}

func (B BooleanValue) Or(other BooleanValue) BooleanValue {
	return BooleanValue{val: B.val || other.val}
}

func (B BooleanValue) And(other BooleanValue) BooleanValue {
	return BooleanValue{val: B.val && other.val}
}

func (B BooleanValue) Not() BooleanValue {
	return BooleanValue{val: !B.val}
}

func (B BooleanValue) Eq(other Value) BooleanValue {
	switch other := other.(type) {
	case BooleanValue:
		return B.And(other)
	default:
		panic("Boolean type mismatch")
	}
}

func (B BooleanValue) NEq(other Value) BooleanValue {
	switch other := other.(type) {
	case BooleanValue:
		return B.Eq(other.Not())
	default:
		panic("Boolean type mismatch")
	}
}

func (B BooleanValue) Lt(other Value) BooleanValue {
	panic("Boolean type mismatch")
}

func (B BooleanValue) Lte(other Value) BooleanValue {
	panic("Boolean type mismatch")
}

func (B BooleanValue) Gt(other Value) BooleanValue {
	panic("Boolean type mismatch")
}

func (B BooleanValue) Gte(other Value) BooleanValue {
	panic("Boolean type mismatch")
}

type IntValue struct{ val int64 }

func (i IntValue) Eq(other Value) BooleanValue {
	switch other := other.(type) {
	case IntValue:
		return BooleanValue{val: i.val == other.val}
	case FloatValue:
		return BooleanValue{val: float64(i.val) == other.val}
	default:
		panic("Boolean type mismatch")
	}
}

func (i IntValue) NEq(other Value) BooleanValue {
	//TODO implement me
	panic("implement me")
}

func (i IntValue) Lt(other Value) BooleanValue {
	//TODO implement me
	panic("implement me")
}

func (i IntValue) Lte(other Value) BooleanValue {
	//TODO implement me
	panic("implement me")
}

func (i IntValue) Gt(other Value) BooleanValue {
	//TODO implement me
	panic("implement me")
}

func (i IntValue) Gte(other Value) BooleanValue {
	//TODO implement me
	panic("implement me")
}

type StringValue struct{ val string }

func (s StringValue) Eq(other Value) BooleanValue {
	//TODO implement me
	panic("implement me")
}

func (s StringValue) NEq(other Value) BooleanValue {
	//TODO implement me
	panic("implement me")
}

func (s StringValue) Lt(other Value) BooleanValue {
	//TODO implement me
	panic("implement me")
}

func (s StringValue) Lte(other Value) BooleanValue {
	//TODO implement me
	panic("implement me")
}

func (s StringValue) Gt(other Value) BooleanValue {
	//TODO implement me
	panic("implement me")
}

func (s StringValue) Gte(other Value) BooleanValue {
	//TODO implement me
	panic("implement me")
}

type FloatValue struct{ val float64 }

func (f FloatValue) Eq(other Value) BooleanValue {
	//TODO implement me
	panic("implement me")
}

func (f FloatValue) NEq(other Value) BooleanValue {
	//TODO implement me
	panic("implement me")
}

func (f FloatValue) Lt(other Value) BooleanValue {
	//TODO implement me
	panic("implement me")
}

func (f FloatValue) Lte(other Value) BooleanValue {
	//TODO implement me
	panic("implement me")
}

func (f FloatValue) Gt(other Value) BooleanValue {
	//TODO implement me
	panic("implement me")
}

func (f FloatValue) Gte(other Value) BooleanValue {
	//TODO implement me
	panic("implement me")
}

type NullValue struct{}

func (n NullValue) Eq(other Value) BooleanValue {
	//TODO implement me
	panic("implement me")
}

func (n NullValue) NEq(other Value) BooleanValue {
	//TODO implement me
	panic("implement me")
}

func (n NullValue) Lt(other Value) BooleanValue {
	//TODO implement me
	panic("implement me")
}

func (n NullValue) Lte(other Value) BooleanValue {
	//TODO implement me
	panic("implement me")
}

func (n NullValue) Gt(other Value) BooleanValue {
	//TODO implement me
	panic("implement me")
}

func (n NullValue) Gte(other Value) BooleanValue {
	//TODO implement me
	panic("implement me")
}
