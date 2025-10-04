package models

type Transform struct{}

type SelectCondition struct {
	Field          []string
	StreamName     *string // nil when no alias
	Transforms     []Transform
	GeneratedAlias string
	Alias          string
}
