package parser

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

func splitColumnName(s string) (*string, string) {
	parts := strings.Split(s, ".")
	if len(parts) == 1 {
		return nil, s
	}
	source := parts[0]
	return &source, strings.Join(parts[1:], ".")
}

type ASTBuilderVisitor struct {
	*BaseNSQLVisitor
	errors []SemanticError
}

type SemanticError struct {
	Line    int
	Column  int
	Message string
}

func NewASTBuilderVisitor() *ASTBuilderVisitor {
	return &ASTBuilderVisitor{
		BaseNSQLVisitor: &BaseNSQLVisitor{},
		errors:          make([]SemanticError, 0),
	}
}

func (v *ASTBuilderVisitor) HasErrors() bool {
	return len(v.errors) > 0
}

func (v *ASTBuilderVisitor) GetErrors() []SemanticError {
	return v.errors
}

func (v *ASTBuilderVisitor) ClearErrors() {
	v.errors = v.errors[:0]
}

func (v *ASTBuilderVisitor) addError(ctx interface{}, message string) {
	// Try to get position info from context
	var line, column int

	// This depends On your specific context type, but usually:
	if ctxWithToken, ok := ctx.(interface {
		GetStart() interface {
			GetLine() int
			GetColumn() int
		}
	}); ok {
		line = ctxWithToken.GetStart().GetLine()
		column = ctxWithToken.GetStart().GetColumn()
	}

	v.errors = append(v.errors, SemanticError{
		Line:    line,
		Column:  column,
		Message: message,
	})
}

func (v *ASTBuilderVisitor) VisitQuery(ctx *QueryContext) interface{} {
	if selectStmt := ctx.SelectStatement(); selectStmt != nil {
		return selectStmt.Accept(v)
	}
	return nil
}

func (v *ASTBuilderVisitor) VisitSelectStatement(ctx *SelectStatementContext) interface{} {
	selectNode := &SelectNode{}
	if tableExpr := ctx.TableExpression(); tableExpr != nil {
		source := tableExpr.Accept(v).(Node)
		selectNode.Source = source
	}

	// Get fields (SELECT clause) - selectList
	if selectList := ctx.SelectList(); selectList != nil {
		fields := selectList.Accept(v).([]Column)
		selectNode.Fields = fields
	}

	// If there's a WhereClause, put it as the source for the Select.
	if whereClause := ctx.WhereClause(); whereClause != nil {
		whereNode := whereClause.Accept(v).(WhereNode)
		whereNode.Source = selectNode.Source
		selectNode.Source = whereNode
	}

	return selectNode
}

func (v *ASTBuilderVisitor) VisitTableExpression(ctx *TableExpressionContext) interface{} {
	streamName := ctx.IDENTIFIER(0).GetText()

	source := &Source{
		StreamName: streamName,
		Alias:      nil,
	}

	// It almost doesn't matter if the AS is there or not.
	if ctx.IDENTIFIER(1) != nil {
		alias := ctx.IDENTIFIER(1).GetText()
		source.Alias = &alias
	}

	if ctx.JoinClause(0) != nil {
		jw := ctx.JoinClause(0).Accept(v).(JoinWindow)
		jw.LHS = source
		return jw
	} else {
		return source
	}
}

func (v *ASTBuilderVisitor) VisitJoinClause(ctx *JoinClauseContext) interface{} {
	jw := JoinWindow{
		LHS:    nil,
		RHS:    ctx.TableExpression().Accept(v).(Node),
		Within: ctx.JoinWindow().Accept(v).(time.Duration),
		On:     ctx.Expression().Accept(v).(Evaluatable),
	}
	return jw
}

func (v *ASTBuilderVisitor) VisitJoinWindow(ctx *JoinWindowContext) interface{} {
	numberText := ctx.NUMBER().GetText()
	value, _ := strconv.Atoi(numberText)
	timeUnit := ctx.TimeUnit().Accept(v).(time.Duration)
	return timeUnit * time.Duration(value)
}

func (v *ASTBuilderVisitor) VisitTimeUnit(ctx *TimeUnitContext) interface{} {
	unitText := ctx.GetText()

	switch unitText {
	case "HOUR", "HOURS":
		return time.Hour
	case "MINUTE", "MINUTES":
		return time.Minute
	case "SECOND", "SECONDS":
		return time.Second
	case "DAY", "DAYS":
		return 24 * time.Hour
	default:
		v.addError(ctx, fmt.Sprintf("Unknown time unit: %s", unitText))
		return time.Second // default
	}
}

func (v *ASTBuilderVisitor) VisitSelectList(ctx *SelectListContext) interface{} {
	var columns []Column

	for _, itemCtx := range ctx.AllSelectItem() {
		if itemCtx != nil {
			column := itemCtx.Accept(v).(Column) // Use Accept
			columns = append(columns, column)
		}
	}

	return columns
}

func (v *ASTBuilderVisitor) VisitWhereClause(ctx *WhereClauseContext) interface{} {
	whereClause := WhereNode{}
	whereClause.Filter = ctx.Expression().Accept(v).(Evaluatable)
	return whereClause
}

func (v *ASTBuilderVisitor) VisitSelectItem(ctx *SelectItemContext) interface{} {
	column := Column{}

	// Check for wildcard first (your grammar has: selectItem : expression (AS? IDENTIFIER)? | '*' ;)
	if ctx.GetText() == "*" {
		column.Field = "*"
		return column
	}

	// Handle expressions (field names, etc.)
	if expr := ctx.Expression(); expr != nil {
		column.Source, column.Field = splitColumnName(expr.GetText())
	}

	// Handle alias (AS clause)
	if ctx.IDENTIFIER() != nil {
		alias := ctx.IDENTIFIER().GetText()
		column.Alias = &alias
	}
	return column
}
