package clickhouse_transpiler

import (
	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	traceql_ir "github.com/metrico/qryn/v5/reader/traceql/traceql_ir"
)

type simpleExpressionPlanner struct {
	selector *traceql_ir.Selector
	prefix   string
	// operand is unset only for a whole-query `{}` read by the attribute-less planner.
	operand bool

	//Analyze results
	termIdx []*traceql_ir.Comparison
	cond    *condition

	terms map[string]int
}

// planner plans the selector's span rows: every span in range for `{}`.
func (p *simpleExpressionPlanner) planner() shared.SQLRequestPlanner {
	p.analyze()
	if p.selector.Filter != nil {
		return &AttrConditionPlanner{
			Main:  NewInitIndexPlanner(true),
			Terms: p.termIdx,
			Conds: p.cond,
		}
	}
	return &AllSpansPlanner{}
}

// planEval estimates the selector's cost; a whole-query `{}` (operand unset)
// estimates the attribute-less planner.
func (p *simpleExpressionPlanner) planEval() shared.SQLRequestPlanner {
	p.analyze()
	if p.selector.Filter != nil {
		return &AttrConditionEvaluatorPlanner{
			Main: &AttrConditionPlanner{
				Main:  NewInitIndexPlanner(true),
				Terms: p.termIdx,
				Conds: p.cond,
			},
			Prefix: p.prefix,
		}
	}
	if p.operand {
		return &AllSpansEvaluatorPlanner{Prefix: p.prefix}
	}
	return &AttrlessEvaluatorPlanner{Prefix: p.prefix}
}

func (p *simpleExpressionPlanner) analyze() {
	p.terms = make(map[string]int)
	p.cond = p.analyzeCond(p.selector.Filter)
}

func (p *simpleExpressionPlanner) analyzeCond(exp traceql_ir.FilterExpr) *condition {
	switch exp := exp.(type) {
	case *traceql_ir.Comparison:
		term := exp.String()
		if p.terms[term] != 0 {
			return &condition{simpleIdx: p.terms[term] - 1}
		}
		p.termIdx = append(p.termIdx, exp)
		p.terms[term] = len(p.termIdx)
		return &condition{simpleIdx: len(p.termIdx) - 1}
	case *traceql_ir.FilterOp:
		return &condition{
			simpleIdx: -1,
			op:        exp.Op,
			complex:   []*condition{p.analyzeCond(exp.LHS), p.analyzeCond(exp.RHS)},
		}
	}
	return nil
}
