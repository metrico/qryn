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

// Pipeline is a sequence of elements joined by `|`. A parenthesised pipeline
// of two or more elements is a spanset expression.
type Pipeline struct {
	Elements []Element
}

// Element is one stage of a pipeline: a spanset expression or a scalar filter.
type Element interface {
	isElement()
}

// SpansetExpr is an expression that yields spansets: a selector, a group, a
// parenthesised pipeline or a binary spanset operation.
type SpansetExpr interface {
	Element
	isSpansetExpr()
}

// Selector is `{filter}`.
type Selector struct {
	// Filter is nil for `{}`.
	Filter FilterExpr
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

	OpChild        SpansetOperator = ">"
	OpParent       SpansetOperator = "<"
	OpSibling      SpansetOperator = "~"
	OpNotChild     SpansetOperator = "!>"
	OpNotParent    SpansetOperator = "!<"
	OpNotSibling   SpansetOperator = "!~"
	OpUnionChild   SpansetOperator = "&>"
	OpUnionParent  SpansetOperator = "&<"
	OpUnionSibling SpansetOperator = "&~"

	OpDescendant      SpansetOperator = ">>"
	OpAncestor        SpansetOperator = "<<"
	OpNotDescendant   SpansetOperator = "!>>"
	OpNotAncestor     SpansetOperator = "!<<"
	OpUnionDescendant SpansetOperator = "&>>"
	OpUnionAncestor   SpansetOperator = "&<<"
)

// IsOneHop reports whether o relates a span to its parent or siblings.
func (o SpansetOperator) IsOneHop() bool {
	switch o {
	case OpChild, OpParent, OpSibling, OpNotChild, OpNotParent, OpNotSibling,
		OpUnionChild, OpUnionParent, OpUnionSibling:
		return true
	}
	return false
}

// IsTransitive reports whether o relates a span to its ancestors or descendants.
func (o SpansetOperator) IsTransitive() bool {
	switch o {
	case OpDescendant, OpAncestor, OpNotDescendant, OpNotAncestor, OpUnionDescendant, OpUnionAncestor:
		return true
	}
	return false
}

// IsStructural reports whether o relates spans through the span tree.
func (o SpansetOperator) IsStructural() bool {
	return o.IsOneHop() || o.IsTransitive()
}

// HasStructural reports whether any spanset expression of q uses a structural operator.
func (q *Query) HasStructural() bool {
	return q.Pipeline.hasStructural()
}

func (p *Pipeline) hasStructural() bool {
	for _, element := range p.Elements {
		if expr, ok := element.(SpansetExpr); ok && hasStructural(expr) {
			return true
		}
	}
	return false
}

func hasStructural(expr SpansetExpr) bool {
	switch expr := expr.(type) {
	case *Group:
		return hasStructural(expr.Expr)
	case *Pipeline:
		return expr.hasStructural()
	case *SpansetOp:
		return expr.Op.IsStructural() || hasStructural(expr.LHS) || hasStructural(expr.RHS)
	}
	return false
}

// SpansetOp is `LHS Op RHS` over spanset expressions.
type SpansetOp struct {
	Op  SpansetOperator
	LHS SpansetExpr
	RHS SpansetExpr
}

func (*Selector) isElement()     {}
func (*Group) isElement()        {}
func (*Pipeline) isElement()     {}
func (*SpansetOp) isElement()    {}
func (*ScalarFilter) isElement() {}

func (*Selector) isSpansetExpr()  {}
func (*Group) isSpansetExpr()     {}
func (*Pipeline) isSpansetExpr()  {}
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

// ScalarFilter is the pipeline stage `LHS Op RHS` over scalar expressions.
type ScalarFilter struct {
	Op  string
	LHS ScalarExpr
	RHS ScalarExpr
}

func (f *ScalarFilter) String() string {
	return f.LHS.String() + " " + f.Op + " " + f.RHS.String()
}

// ScalarExpr is an aggregate, a number literal or arithmetic over them.
type ScalarExpr interface {
	isScalarExpr()
	String() string
}

// Aggregate is `Fn(Attr)` over a spanset's spans; Attr is empty for count().
type Aggregate struct {
	Fn   string
	Attr string
}

// String is the aggregate's canonical form, which names its value in results.
func (a *Aggregate) String() string {
	return a.Fn + "(" + a.Attr + ")"
}

// Static is a number literal as written: an integer, a float or a duration.
type Static struct {
	Kind ValueKind
	Text string
}

func (s *Static) String() string {
	return s.Text
}

// ScalarOp is `LHS Op RHS` with an arithmetic Op.
type ScalarOp struct {
	Op  string
	LHS ScalarExpr
	RHS ScalarExpr
}

func (o *ScalarOp) String() string {
	return "(" + o.LHS.String() + " " + o.Op + " " + o.RHS.String() + ")"
}

func (*Aggregate) isScalarExpr() {}
func (*Static) isScalarExpr()    {}
func (*ScalarOp) isScalarExpr()  {}

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
