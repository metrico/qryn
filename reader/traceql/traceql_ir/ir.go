// Package traceql_ir is the lowered form of a TraceQL query that the search,
// metrics and autocomplete planners consume.
package traceql_ir

import (
	traceql_parser "github.com/metrico/qryn/v5/reader/traceql/traceql_parser"
)

// Query is a lowered TraceQL query: a pipeline followed by the optional
// metrics function, hints and second stage.
type Query struct {
	Pipeline    Pipeline
	Metrics     *MetricsFn
	Hints       []Hint
	SecondStage *SecondStage
}

// Pipeline is a sequence of elements joined by `|`.
type Pipeline struct {
	Elements []Element
}

// Element is one stage of a pipeline.
type Element interface {
	isElement()
}

// SpansetExpr is an expression that yields spansets: a selector, a group or a
// binary spanset operation.
type SpansetExpr interface {
	Element
	isSpansetExpr()
}

// Selector is `{filter}`, with the scalar filter written directly after it.
type Selector struct {
	// Filter is nil for `{}`.
	Filter     FilterExpr
	Aggregator *Aggregator
}

// Group is a parenthesised spanset expression.
type Group struct {
	Expr SpansetExpr
}

// SpansetOperator is a binary operator between two spanset expressions.
type SpansetOperator string

const (
	OpAnd SpansetOperator = "&&"
	OpOr  SpansetOperator = "||"
)

// SpansetOp is `LHS Op RHS` over spanset expressions.
type SpansetOp struct {
	Op  SpansetOperator
	LHS SpansetExpr
	RHS SpansetExpr
}

func (*Selector) isElement()  {}
func (*Group) isElement()     {}
func (*SpansetOp) isElement() {}

func (*Selector) isSpansetExpr()  {}
func (*Group) isSpansetExpr()     {}
func (*SpansetOp) isSpansetExpr() {}

// FilterExpr is the condition inside `{…}`.
type FilterExpr interface {
	isFilterExpr()
}

// FilterOp is `LHS Op RHS` with Op `&&` or `||`.
type FilterOp struct {
	Op  string
	LHS FilterExpr
	RHS FilterExpr
}

// Comparison is `Attr Op Value`. Op and Value are empty for a bare attribute.
type Comparison struct {
	Attr  string
	Op    string
	Value Value
}

func (*FilterOp) isFilterExpr()   {}
func (*Comparison) isFilterExpr() {}

func (c *Comparison) String() string {
	return c.Attr + " " + c.Op + " " + c.Value.Text
}

// ValueKind is the lexical kind of a comparison's right-hand side.
type ValueKind int

const (
	ValueNone ValueKind = iota
	ValueNil
	ValueBool
	ValueDuration
	ValueNumber
	ValueString
	// ValueIdent is an unquoted word such as `error` in `status = error`.
	ValueIdent
)

// Value is a comparison's right-hand side as written. Strings keep their quotes.
type Value struct {
	Kind ValueKind
	Text string
}

// Unquote returns the content of a quoted string value.
func (v Value) Unquote() (string, error) {
	return (&traceql_parser.QuotedString{Str: v.Text}).Unquote()
}

// Aggregator is a scalar filter `| fn(Attr) Cmp Value`.
type Aggregator struct {
	Fn   string
	Attr string
	Cmp  string
	// Value is the number as written, with its duration unit if any.
	Value string
}

// MetricsFn is the terminal metrics function, e.g. `| rate() by (name)`.
type MetricsFn struct {
	Fn         string
	Attr       string
	Percentile *float64
	Compare    *CompareArgs
	By         []string
}

// CompareArgs are the arguments of `compare({filter}, count, from, to)`.
type CompareArgs struct {
	// Filter is nil for `{}`.
	Filter       FilterExpr
	Count        int
	BaselineFrom *int64
	BaselineTo   *int64
}

// Hint is one `key=value` of `with(...)`.
type Hint struct {
	Key   string
	Value string
}

// SecondStage is `| topk(n)`, `| bottomk(n)` or `| select(labels...)` after the
// metrics function.
type SecondStage struct {
	Fn     string
	Count  int
	Labels []string
	Hints  []Hint
}
