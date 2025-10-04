package parser

import (
	"fmt"
	"strconv"
	"strings"
)

func (v *ASTBuilderVisitor) VisitAndExpression(ctx *AndExpressionContext) interface{} {
	return And{
		ctx.Expression(0).Accept(v).(Evaluatable),
		ctx.Expression(1).Accept(v).(Evaluatable),
	}
}

func (v *ASTBuilderVisitor) VisitOrExpression(ctx *OrExpressionContext) interface{} {
	return Or{
		ctx.Expression(0).Accept(v).(Evaluatable),
		ctx.Expression(1).Accept(v).(Evaluatable),
	}
}

func (v *ASTBuilderVisitor) VisitNotExpression(ctx *NotExpressionContext) interface{} {
	return Negate{
		ctx.Expression().Accept(v).(Evaluatable),
	}
}

func (v *ASTBuilderVisitor) VisitComparisonExpression(ctx *ComparisonExpressionContext) interface{} {
	switch ctx.ComparisonOp().GetText() {
	case "!=":
		return Negate{EQ{
			ctx.Expression(0).Accept(v).(Evaluatable),
			ctx.Expression(1).Accept(v).(Evaluatable),
		}}
	case "=":
		return EQ{
			ctx.Expression(0).Accept(v).(Evaluatable),
			ctx.Expression(1).Accept(v).(Evaluatable),
		}
	default:
		v.addError(ctx, fmt.Sprintf(`Operator "%s" not supported`, ctx.ComparisonOp().GetText()))
		return nil
	}
}

func (v *ASTBuilderVisitor) VisitLikeExpression(ctx *LikeExpressionContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *ASTBuilderVisitor) VisitInExpression(ctx *InExpressionContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *ASTBuilderVisitor) VisitQualifiedIdentifierExpression(ctx *QualifiedIdentifierExpressionContext) interface{} {
	text := ctx.GetText()

	// Handle qualified names like "table.field"
	if strings.Contains(text, ".") {
		parts := strings.SplitN(text, ".", 2)
		source := parts[0]
		field := parts[1]
		return FieldReference{
			Source: &source,
			Field:  field,
		}
	}

	// Simple field reference
	return FieldReference{
		Source: nil,
		Field:  text,
	}
}

func (v *ASTBuilderVisitor) VisitStringExpression(ctx *StringExpressionContext) interface{} {
	return Constant{value: StringValue{val: ctx.GetText()}}
}

func (v *ASTBuilderVisitor) VisitNumberExpression(ctx *NumberExpressionContext) interface{} {
	numStr := ctx.NUMBER().GetText()
	if !strings.Contains(numStr, ".") && !strings.ContainsAny(numStr, "eE") {
		if val, err := strconv.ParseInt(numStr, 10, 64); err == nil {
			return Constant{value: IntValue{val: val}}
		}
	}

	// Fall back to float
	if val, err := strconv.ParseFloat(numStr, 64); err == nil {
		return Constant{value: FloatValue{val: val}}
	}
	v.addError(ctx, fmt.Sprintf(`Operator "%s" not supported`, numStr))
	return Constant{NullValue{}}
}

func (v *ASTBuilderVisitor) VisitParenthesizedExpression(ctx *ParenthesizedExpressionContext) interface{} {
	return v.VisitChildren(ctx)
}
