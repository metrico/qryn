package traceql_ir

import (
	traceql_parser "github.com/metrico/qryn/v5/reader/traceql/traceql_parser"
)

// Parse parses and lowers a TraceQL query.
func Parse(query string) (*Query, error) {
	script, err := traceql_parser.Parse(query)
	if err != nil {
		return nil, err
	}
	return Lower(script), nil
}

// Lower converts a parsed script into its IR.
func Lower(script *traceql_parser.TraceQLScript) *Query {
	q := &Query{
		Pipeline: lowerPipeline(script.Pipeline.Head, script.Pipeline.Stages),
		Metrics:  lowerMetricsFn(script.MetricsFn),
		Hints:    lowerHints(script.WithHints),
	}
	if s := script.SecondStage; s != nil {
		q.SecondStage = &SecondStage{Fn: s.Fn, Count: s.Count, Labels: s.Labels, Hints: lowerHints(s.WithHints)}
	}
	return q
}

func lowerPipeline(head traceql_parser.PipelineElement, stages []traceql_parser.PipelineElement) Pipeline {
	return lowerStages(lowerElement(head), stages)
}

func lowerStages(head Element, stages []traceql_parser.PipelineElement) Pipeline {
	res := Pipeline{Elements: []Element{head}}
	for _, stage := range stages {
		res.Elements = append(res.Elements, lowerElement(stage))
	}
	return res
}

func lowerElement(e traceql_parser.PipelineElement) Element {
	if e.ScalarFilter != nil {
		return lowerScalarFilter(e.ScalarFilter)
	}
	return lowerSpansetExpr(e.Spanset)
}

// lowerSpansetExpr turns an operand chain into a tree. Structural operators
// bind tighter than `&&` and `||`, which share one level; all are
// left-associative.
func lowerSpansetExpr(expr *traceql_parser.SpansetExpr) SpansetExpr {
	var (
		res    SpansetExpr
		joinOp SpansetOperator
	)
	term := lowerOperand(expr.Head)
	flush := func() {
		if res == nil {
			res = term
		} else {
			res = &SpansetOp{Op: joinOp, LHS: res, RHS: term}
		}
	}
	for _, next := range expr.Ops {
		op := SpansetOperator(next.Op)
		operand := lowerOperand(next.Operand)
		if op == OpAnd || op == OpOr {
			flush()
			term, joinOp = operand, op
		} else {
			term = &SpansetOp{Op: op, LHS: term, RHS: operand}
		}
	}
	flush()
	return res
}

// lowerOperand lowers a selector, or a parenthesised pipeline: a Group when
// it is a lone spanset expression, else a Pipeline.
func lowerOperand(o traceql_parser.Operand) SpansetExpr {
	if o.Paren == nil {
		return &Selector{Filter: lowerFilter(o.Selector.AttrSelector)}
	}
	head := lowerSpansetExpr(&o.Paren.Head)
	if len(o.Paren.Stages) == 0 {
		return &Group{Expr: head}
	}
	res := lowerStages(head, o.Paren.Stages)
	return &res
}

func lowerScalarFilter(f *traceql_parser.ScalarFilter) *ScalarFilter {
	return &ScalarFilter{Op: f.Op, LHS: lowerScalarExpr(f.LHS), RHS: lowerScalarExpr(f.RHS)}
}

func lowerScalarExpr(e traceql_parser.ScalarExpr) ScalarExpr {
	res := lowerScalarTerm(e.Head)
	for _, op := range e.Ops {
		res = &ScalarOp{Op: op.Op, LHS: res, RHS: lowerScalarTerm(op.Term)}
	}
	return res
}

func lowerScalarTerm(t traceql_parser.ScalarTerm) ScalarExpr {
	res := lowerScalarFactor(t.Head)
	for _, op := range t.Ops {
		res = &ScalarOp{Op: op.Op, LHS: res, RHS: lowerScalarFactor(op.Factor)}
	}
	return res
}

func lowerScalarFactor(f traceql_parser.ScalarFactor) ScalarExpr {
	base := lowerScalarAtom(f.Base)
	if f.Exp == nil {
		return base
	}
	return &ScalarOp{Op: "^", LHS: base, RHS: lowerScalarFactor(*f.Exp)}
}

func lowerScalarAtom(a traceql_parser.ScalarAtom) ScalarExpr {
	switch {
	case a.Aggregate != nil:
		return &Aggregate{Fn: a.Aggregate.Fn, Attr: a.Aggregate.Attr}
	case a.Literal != nil:
		if a.Literal.Unit != "" {
			return &Static{Kind: ValueDuration, Text: a.Literal.Num + a.Literal.Unit}
		}
		return &Static{Kind: ValueNumber, Text: a.Literal.Num}
	}
	return lowerScalarExpr(*a.Paren)
}

func lowerFilter(exp *traceql_parser.AttrSelectorExp) FilterExpr {
	if exp == nil {
		return nil
	}
	var head FilterExpr
	switch {
	case exp.ComplexHead != nil:
		head = lowerFilter(exp.ComplexHead)
	case exp.Head != nil:
		head = &Comparison{Attr: exp.Head.Label, Op: exp.Head.Op, Value: lowerValue(exp.Head.Val)}
	default:
		head = &Comparison{Attr: exp.BoolLiteral}
	}
	if exp.Tail == nil {
		return head
	}
	return &FilterOp{Op: exp.AndOr, LHS: head, RHS: lowerFilter(exp.Tail)}
}

func lowerValue(v traceql_parser.Value) Value {
	switch {
	case v.NilVal != "":
		return Value{Kind: ValueNil, Text: v.NilVal}
	case v.BoolVal != "":
		return Value{Kind: ValueBool, Text: v.BoolVal}
	case v.StrVal != nil:
		return Value{Kind: ValueString, Text: v.StrVal.Str}
	case v.FVal != "":
		return Value{Kind: ValueNumber, Text: v.FVal}
	case v.TimeVal != "":
		return Value{Kind: ValueDuration, Text: v.TimeVal}
	case v.UnquotVal != "":
		return Value{Kind: ValueIdent, Text: v.UnquotVal}
	}
	return Value{}
}

func lowerMetricsFn(m *traceql_parser.MetricsPipelineStage) *MetricsFn {
	if m == nil {
		return nil
	}
	res := &MetricsFn{Fn: m.Fn, Attr: m.Attr, Percentile: m.Percentile}
	if m.By != nil {
		res.By = m.By.Labels
	}
	if c := m.CompSel; c != nil {
		res.Compare = &CompareArgs{
			Filter:       lowerFilter(c.AttrSelector),
			Count:        c.Count,
			BaselineFrom: c.BaselineFrom,
			BaselineTo:   c.BaselineTo,
		}
	}
	return res
}

func lowerHints(w *traceql_parser.WithClause) []Hint {
	if w == nil {
		return nil
	}
	res := make([]Hint, len(w.Hints))
	for i, h := range w.Hints {
		res[i] = Hint{Key: h.Key, Value: h.Value}
	}
	return res
}
