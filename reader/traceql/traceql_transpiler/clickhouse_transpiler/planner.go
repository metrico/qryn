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

func PlanTagsV2(query *traceql_ir.Query) (shared.SQLRequestPlanner, error) {
	return (&planner{query: query}).planTagsV2()
}

func PlanValuesV2(query *traceql_ir.Query, key string) (shared.SQLRequestPlanner, error) {
	return (&planner{query: query}).planValuesV2(key)
}

var (
	errEmptyAggregated = errors.New("requests like `{} | ....` are not supported")
	errEmptyChainHead  = errors.New("requests like `{} || .....` are not supported")
	errEmptyChainTail  = errors.New("requests like `... || {}` are not supported")
	errComplexAutocomp = errors.New("complex requests `{} || {} ...` are not supported")
)

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
	if _, ok := expr.(*traceql_ir.SpansetOp); !ok {
		res, err = (&simpleExpressionPlanner{selector: operandSelector(expr)}).planner()
	} else {
		selectors, ops, chainErr := checkChain(expr, false)
		if chainErr != nil {
			return nil, chainErr
		}
		root := &rootExpressionPlanner{}
		p.planChain(root, root, selectors, ops)
		res, err = root.planner()
	}
	if err != nil {
		return nil, err
	}
	res = &IndexLimitPlanner{res}

	res = NewTracesDataPlanner(res)

	res = &IndexLimitPlanner{res}

	return res, nil
}

func (p *planner) planEval() (shared.SQLRequestPlanner, error) {
	expr, err := p.searchExpr()
	if err != nil {
		return nil, err
	}

	var res shared.SQLRequestPlanner
	if _, ok := expr.(*traceql_ir.SpansetOp); !ok {
		res, err = (&simpleExpressionPlanner{selector: operandSelector(expr), prefix: p.getPrefix()}).planEval()
	} else {
		selectors, ops, chainErr := checkChain(expr, true)
		if chainErr != nil {
			return nil, chainErr
		}
		root := &rootExpressionPlanner{}
		p.planChain(root, root, selectors, ops)
		res, err = root.planEval()
	}
	if err != nil {
		return nil, err
	}
	res = &EvalFinalizerPlanner{Main: res}
	return res, nil
}

func (p *planner) planTagsV2() (shared.SQLRequestPlanner, error) {
	expr, err := p.spansetExpr()
	if err != nil {
		return nil, err
	}
	if _, ok := expr.(*traceql_ir.SpansetOp); ok {
		return nil, errComplexAutocomp
	}
	return (&simpleExpressionPlanner{selector: operandSelector(expr)}).tagsV2Planner()
}

func (p *planner) planValuesV2(key string) (shared.SQLRequestPlanner, error) {
	expr, err := p.spansetExpr()
	if err != nil {
		return nil, err
	}
	if _, ok := expr.(*traceql_ir.SpansetOp); ok {
		return nil, errComplexAutocomp
	}
	return (&simpleExpressionPlanner{selector: operandSelector(expr)}).valuesV2Planner(key)
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
	if group, ok := expr.(*traceql_ir.Group); ok && q.Metrics == nil && len(q.Hints) == 0 && q.SecondStage == nil {
		expr = group.Expr
	}
	return expr, nil
}

func (p *planner) getPrefix() string {
	p.prefix++
	return fmt.Sprintf("_%d", p.prefix)
}

// planChain plans a checked chain from left to right. Every structural operator
// is evaluated as `&&`. An `&&` operand nests into the first operand of the
// current node, and `||` wraps everything planned so far.
func (p *planner) planChain(root iExpressionPlanner, current iExpressionPlanner,
	selectors []*traceql_ir.Selector, ops []traceql_ir.SpansetOperator) {
	selector := selectors[0]
	switch {
	case len(ops) == 0:
		current.addOp(&simpleExpressionPlanner{selector: selector, prefix: p.getPrefix()})
	case ops[0] == traceql_ir.OpOr:
		current.addOp(&simpleExpressionPlanner{
			selector: selector,
			prefix:   p.getPrefix(),
		})
		root.setOps([]iExpressionPlanner{&complexExpressionPlanner{
			prefix:    p.getPrefix(),
			_fn:       string(traceql_ir.OpOr),
			_operands: root.operands(),
		}})
		p.planChain(root, root.operands()[0], selectors[1:], ops[1:])
	default:
		current.addOp(&complexExpressionPlanner{
			prefix: p.getPrefix(),
			_fn:    string(traceql_ir.OpAnd),
			_operands: []iExpressionPlanner{&simpleExpressionPlanner{
				selector: selector,
				prefix:   p.getPrefix(),
			}},
		})
		p.planChain(root, current.operands()[0], selectors[1:], ops[1:])
	}
}

// checkChain returns a chain's selectors and the operators between them. Every
// operand must be a non-empty selector without parentheses, except that a
// parenthesised selector is accepted as the head when groupedHead is set.
func checkChain(expr traceql_ir.SpansetExpr, groupedHead bool) (
	[]*traceql_ir.Selector, []traceql_ir.SpansetOperator, error) {
	operands, ops := chainOf(expr)
	head := operands[0]
	for {
		group, ok := head.(*traceql_ir.Group)
		if !ok {
			break
		}
		if _, ok := group.Expr.(*traceql_ir.SpansetOp); ok {
			return nil, nil, errEmptyChainHead
		}
		head = group.Expr
	}
	headSelector := head.(*traceql_ir.Selector)
	if headSelector.Filter == nil {
		if headSelector.Aggregator != nil {
			return nil, nil, errEmptyAggregated
		}
		return nil, nil, errEmptyChainHead
	}
	selectors := []*traceql_ir.Selector{headSelector}
	for _, operand := range operands[1:] {
		selector, ok := operand.(*traceql_ir.Selector)
		if !ok || selector.Filter == nil {
			return nil, nil, errEmptyChainTail
		}
		selectors = append(selectors, selector)
	}
	if _, ok := operands[0].(*traceql_ir.Group); ok && !groupedHead {
		return nil, nil, errEmptyChainHead
	}
	return selectors, ops, nil
}

// chainOf lists a chain's operands and operators in query order; groups stay whole.
func chainOf(expr traceql_ir.SpansetExpr) ([]traceql_ir.SpansetExpr, []traceql_ir.SpansetOperator) {
	op, ok := expr.(*traceql_ir.SpansetOp)
	if !ok {
		return []traceql_ir.SpansetExpr{expr}, nil
	}
	lhs, lhsOps := chainOf(op.LHS)
	rhs, rhsOps := chainOf(op.RHS)
	ops := append(append(lhsOps, op.Op), rhsOps...)
	return append(lhs, rhs...), ops
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
