package parser

import (
	"fmt"

	"github.com/antlr4-go/antlr/v4"
)

type ErrorListener struct {
	*antlr.DefaultErrorListener
	Errors []string
}

func (el *ErrorListener) SyntaxError(recognizer antlr.Recognizer, offendingSymbol interface{},
	line, column int, msg string, e antlr.RecognitionException) {

	errorMsg := fmt.Sprintf("line %d:%d - %s", line, column, msg)
	el.Errors = append(el.Errors, errorMsg)
}

func ParseSQL(input string) (Node, error) {
	inputStream := antlr.NewInputStream(input)
	lexer := NewNSQLLexer(inputStream)
	tokenStream := antlr.NewCommonTokenStream(lexer, antlr.TokenDefaultChannel)
	parser := NewNSQLParser(tokenStream)

	errorListener := &ErrorListener{}

	lexer.RemoveErrorListeners()
	lexer.AddErrorListener(errorListener)

	parser.RemoveErrorListeners()
	parser.AddErrorListener(errorListener)

	tree := parser.Query()

	fmt.Printf("Tree string representation:\n%s\n", tree.ToStringTree(nil, parser))
	if len(errorListener.Errors) > 0 {
		return nil, fmt.Errorf("parse errors: %v", errorListener.Errors)
	}

	builder := NewASTBuilderVisitor()
	result := tree.Accept(builder)

	if builder.HasErrors() {
		fmt.Println("Semantic errors found:")
		for _, err := range builder.GetErrors() {
			fmt.Printf("  Line %d, Column %d: %s\n", err.Line, err.Column, err.Message)
		}
		return nil, fmt.Errorf("found %d semantic errors", len(builder.GetErrors()))
	}

	if selectNode, ok := result.(*SelectNode); ok {
		return selectNode, nil
	} else {
		return nil, fmt.Errorf("expected SelectNode, got %T", result)
	}
}
