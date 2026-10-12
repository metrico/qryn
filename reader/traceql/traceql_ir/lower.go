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
		Pipeline: Pipeline{Elements: []Element{lowerSpansetExpr(script)}},
		Metrics:  lowerMetricsFn(script.ResolvedMetricsFn()),
		Hints:    lowerHints(script.WithHints),
	}
	if s := script.SecondStage; s != nil {
		q.SecondStage = &SecondStage{Fn: s.Fn, Count: s.Count, Labels: s.Labels, Hints: lowerHints(s.WithHints)}
	}
	return q
}

// lowerSpansetExpr turns the script's operand chain into a tree. Structural
// operators bind tighter than `&&` and `||`, which share one level; all are
// left-associative.
func lowerSpansetExpr(script *traceql_parser.TraceQLScript) SpansetExpr {
	var (
		res    SpansetExpr
		term   SpansetExpr
		joinOp SpansetOperator
		prevOp SpansetOperator
	)
	flush := func() {
		if res == nil {
			res = term
		} else {
			res = &SpansetOp{Op: joinOp, LHS: res, RHS: term}
		}
	}
	for s := script; s != nil; s = s.Tail {
		operand := lowerOperand(s)
		if term == nil {
			term = operand
		} else {
			term = &SpansetOp{Op: prevOp, LHS: term, RHS: operand}
		}
		if s.Op == "" || s.Tail == nil {
			break
		}
		op := SpansetOperator(s.Op)
		if op == OpAnd || op == OpOr {
			flush()
			term, joinOp = nil, op
		} else {
			prevOp = op
		}
	}
	flush()
	return res
}

func lowerOperand(s *traceql_parser.TraceQLScript) SpansetExpr {
	if s.ParenExpr != nil {
		return &Group{Expr: lowerSpansetExpr(s.ParenExpr)}
	}
	res := &Selector{Filter: lowerFilter(s.Head.AttrSelector)}
	if agg := s.Head.Aggregator; agg != nil {
		res.Aggregator = &Aggregator{Fn: agg.Fn, Attr: agg.Attr, Cmp: agg.Cmp, Value: agg.Num + agg.Measurement}
	}
	return res
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
