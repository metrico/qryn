package clickhouse_transpiler

import (
	"errors"
	"fmt"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	traceql_ir "github.com/metrico/qryn/v5/reader/traceql/traceql_ir"
)

func Plan(query *traceql_ir.Query) (shared.SQLRequestPlanner, error) {
	return (&planner{query: query}).plan()
}

func PlanEval(query *traceql_ir.Query) (shared.SQLRequestPlanner, error) {
	return (&planner{query: query}).planEval()
}

// PlanTagsV2 plans the attribute keys of the spans a search for query
// returns, or of every span in range when AllSpans(query).
func PlanTagsV2(query *traceql_ir.Query) (shared.SQLRequestPlanner, error) {
	if AllSpans(query) {
		return &AllTagsRequestPlanner{}, nil
	}
	return planAutocomplete(query, func(result shared.SQLRequestPlanner, prefix string) shared.SQLRequestPlanner {
		return &SelectTagsPlanner{Result: result, Prefix: prefix}
	})
}

// PlanValuesV2 plans the values of attribute key of the spans a search for
// query returns, or of every span in range when AllSpans(query).
func PlanValuesV2(query *traceql_ir.Query, key string) (shared.SQLRequestPlanner, error) {
	if AllSpans(query) {
		return &AllValuesRequestPlanner{Key: key}, nil
	}
	return planAutocomplete(query, func(result shared.SQLRequestPlanner, prefix string) shared.SQLRequestPlanner {
		return &SelectValuesPlanner{Result: result, Prefix: prefix, Key: key}
	})
}

// planAutocomplete plans the result spans of query and reads them with attrs.
func planAutocomplete(query *traceql_ir.Query,
	attrs func(result shared.SQLRequestPlanner, prefix string) shared.SQLRequestPlanner) (shared.SQLRequestPlanner, error) {
	p := &planner{query: query}
	expr, err := p.spansetExpr()
	if err != nil {
		return nil, err
	}
	result, err := p.planSpansetExpr(expr)
	if err != nil {
		return nil, err
	}
	return p.structuralGate(attrs(result, p.getPrefix())), nil
}

// AllSpans reports whether query selects every span: no query, or a lone `{}`.
func AllSpans(query *traceql_ir.Query) bool {
	if query == nil {
		return true
	}
	expr, err := (&planner{query: query}).spansetExpr()
	if err != nil {
		return false
	}
	for {
		group, ok := expr.(*traceql_ir.Group)
		if !ok {
			break
		}
		expr = group.Expr
	}
	selector, ok := expr.(*traceql_ir.Selector)
	return ok && selector.Filter == nil && selector.Aggregator == nil
}

var errEmptyAggregated = errors.New("requests like `{} | ....` are not supported")

type planner struct {
	query  *traceql_ir.Query
	prefix int
}

func (p *planner) plan() (shared.SQLRequestPlanner, error) {
	expr, err := p.searchExpr()
	if err != nil {
		return nil, err
	}
	var res shared.SQLRequestPlanner
	if _, ok := expr.(*traceql_ir.SpansetOp); ok {
		res, err = p.planSpansetExpr(expr)
	} else {
		res, err = (&simpleExpressionPlanner{selector: operandSelector(expr)}).planner()
	}
	if err != nil {
		return nil, err
	}
	res = &SearchSpansetPlanner{Main: res}

	res = &IndexLimitPlanner{res}

	res = NewTracesDataPlanner(res)

	res = &IndexLimitPlanner{res}

	return p.structuralGate(res), nil
}

func (p *planner) planEval() (shared.SQLRequestPlanner, error) {
	expr, err := p.searchExpr()
	if err != nil {
		return nil, err
	}
	var res shared.SQLRequestPlanner
	if _, ok := expr.(*traceql_ir.SpansetOp); ok {
		res, err = p.planOperandsEval(expr)
	} else {
		res, err = (&simpleExpressionPlanner{selector: operandSelector(expr), prefix: p.getPrefix()}).planEval()
	}
	if err != nil {
		return nil, err
	}
	return &EvalFinalizerPlanner{Main: res}, nil
}

// planSpansetExpr plans a spanset operator tree into span rows.
func (p *planner) planSpansetExpr(expr traceql_ir.SpansetExpr) (shared.SQLRequestPlanner, error) {
	return planSpansetTree(p, expr,
		func(op *simpleExpressionPlanner) (shared.SQLRequestPlanner, error) { return op.planner() },
		func(op traceql_ir.SpansetOperator, lhs, rhs shared.SQLRequestPlanner) shared.SQLRequestPlanner {
			switch op {
			case traceql_ir.OpAnd:
				return &TraceAndPlanner{LHS: lhs, RHS: rhs, Prefix: p.getPrefix()}
			case traceql_ir.OpOr:
				return &TraceOrPlanner{LHS: lhs, RHS: rhs, Prefix: p.getPrefix()}
			}
			if op.IsOneHop() {
				return &OneHopPlanner{Op: op, LHS: lhs, RHS: rhs, Prefix: p.getPrefix()}
			}
			return &TransitivePlanner{Op: op, LHS: lhs, RHS: rhs, Prefix: p.getPrefix()}
		})
}

// planOperandsEval estimates the cost of every operand of a spanset operator tree.
func (p *planner) planOperandsEval(expr traceql_ir.SpansetExpr) (shared.SQLRequestPlanner, error) {
	operands, err := planSpansetTree(p, expr,
		func(op *simpleExpressionPlanner) ([]shared.SQLRequestPlanner, error) {
			eval, err := op.planEval()
			return []shared.SQLRequestPlanner{eval}, err
		},
		func(_ traceql_ir.SpansetOperator, lhs, rhs []shared.SQLRequestPlanner) []shared.SQLRequestPlanner {
			return append(lhs, rhs...)
		})
	if err != nil {
		return nil, err
	}
	return &ComplexEvalOrPlanner{Operands: operands, Prefix: p.getPrefix()}, nil
}

// planSpansetTree folds a spanset operator tree: leaf plans each operand and
// join combines the two sides of an operator. Groups plan as their content.
func planSpansetTree[T any](p *planner, expr traceql_ir.SpansetExpr,
	leaf func(*simpleExpressionPlanner) (T, error),
	join func(traceql_ir.SpansetOperator, T, T) T) (T, error) {
	switch expr := expr.(type) {
	case *traceql_ir.Group:
		return planSpansetTree(p, expr.Expr, leaf, join)
	case *traceql_ir.SpansetOp:
		var zero T
		lhs, err := planSpansetTree(p, expr.LHS, leaf, join)
		if err != nil {
			return zero, err
		}
		rhs, err := planSpansetTree(p, expr.RHS, leaf, join)
		if err != nil {
			return zero, err
		}
		return join(expr.Op, lhs, rhs), nil
	}
	op, err := p.operand(expr.(*traceql_ir.Selector))
	if err != nil {
		var zero T
		return zero, err
	}
	return leaf(op)
}

// operand plans one selector of a spanset operator.
func (p *planner) operand(selector *traceql_ir.Selector) (*simpleExpressionPlanner, error) {
	res := &simpleExpressionPlanner{selector: selector, prefix: p.getPrefix(), operand: true}
	if err := res.check(); err != nil {
		return nil, err
	}
	return res, nil
}

// spansetExpr returns the query's only pipeline element.
func (p *planner) spansetExpr() (traceql_ir.SpansetExpr, error) {
	elements := p.query.Pipeline.Elements
	if len(elements) == 1 {
		if expr, ok := elements[0].(traceql_ir.SpansetExpr); ok {
			return expr, nil
		}
	}
	return nil, &shared.NotSupportedError{Msg: "pipeline stages are not supported"}
}

// searchExpr is the expression search plans. Parentheses around the whole
// query are dropped unless a metrics function, hints or a second stage follow.
func (p *planner) searchExpr() (traceql_ir.SpansetExpr, error) {
	expr, err := p.spansetExpr()
	if err != nil {
		return nil, err
	}
	q := p.query
	if q.Metrics != nil || len(q.Hints) > 0 || q.SecondStage != nil {
		return expr, nil
	}
	for {
		group, ok := expr.(*traceql_ir.Group)
		if !ok {
			return expr, nil
		}
		expr = group.Expr
	}
}

// structuralGate wraps res in StructuralQueryPlanner when the query has
// structural operators.
func (p *planner) structuralGate(res shared.SQLRequestPlanner) shared.SQLRequestPlanner {
	if p.query.HasStructural() {
		return &StructuralQueryPlanner{Main: res}
	}
	return res
}

func (p *planner) getPrefix() string {
	p.prefix++
	return fmt.Sprintf("_%d", p.prefix)
}

// operandSelector is the selector a lone operand plans as. A group plans as `{}`.
func operandSelector(expr traceql_ir.SpansetExpr) *traceql_ir.Selector {
	if selector, ok := expr.(*traceql_ir.Selector); ok {
		return selector
	}
	return &traceql_ir.Selector{}
}

type condition struct {
	simpleIdx int // index of term; -1 means complex

	op      string
	complex []*condition
}
