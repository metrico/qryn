package clickhouse_transpiler

import (
	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	traceql_ir "github.com/metrico/qryn/v5/reader/traceql/traceql_ir"
)

type simpleExpressionPlanner struct {
	selector *traceql_ir.Selector
	prefix   string

	//Analyze results
	termIdx []*traceql_ir.Comparison
	cond    *condition
	aggAttr string

	terms map[string]int
}

func (p *simpleExpressionPlanner) addOp(selector iExpressionPlanner) {
}

func (p *simpleExpressionPlanner) setOps(selector []iExpressionPlanner) {
}

func (p *simpleExpressionPlanner) fn() string {
	return ""
}

func (p *simpleExpressionPlanner) operands() []iExpressionPlanner {
	return nil
}

func (p *simpleExpressionPlanner) tagsV2Planner()(shared.SQLRequestPlanner, error) {
	err := p.check()
	if err != nil {
		return nil, err
	}

	p.analyze()

	var res shared.SQLRequestPlanner = &AttrConditionPlanner{
		Main:           NewInitIndexPlanner(false),
		Terms:          p.termIdx,
		Conds:          p.cond,
		AggregatedAttr: p.aggAttr,
	}
	res = &SelectTagsPlanner{Main: res}
	return res, nil
}

func (p *simpleExpressionPlanner) valuesV2Planner(key string) (shared.SQLRequestPlanner, error) {
	err := p.check()
	if err != nil {
		return nil, err
	}

	p.analyze()

	if p.cond == nil {
		return &AllValuesRequestPlanner{Key: key}, nil
	}

	var res shared.SQLRequestPlanner = &AttrConditionPlanner{
		Main:           NewInitIndexPlanner(false),
		Terms:          p.termIdx,
		Conds:          p.cond,
		AggregatedAttr: p.aggAttr,
	}
	res = &SelectValuesRequestPlanner{
		Main: res,
		Key:  key,
	}
	return res, nil
}

func (p *simpleExpressionPlanner) planner() (shared.SQLRequestPlanner, error) {
	err := p.check()
	if err != nil {
		return nil, err
	}

	p.analyze()
	var res shared.SQLRequestPlanner
	if p.selector.Filter != nil {
		res = &AttrConditionPlanner{
			Main:           NewInitIndexPlanner(false),
			Terms:          p.termIdx,
			Conds:          p.cond,
			AggregatedAttr: p.aggAttr,
		}
	} else {
		res = NewAttrlessConditionPlanner()
	}

	res = &IndexGroupByPlanner{Main: res, Prefix: p.prefix}

	if agg := p.selector.Aggregator; agg != nil {
		res = &AggregatorPlanner{
			Main:       res,
			Fn:         agg.Fn,
			Attr:       p.aggAttr,
			CompareFn:  agg.Cmp,
			CompareVal: agg.Value,
			Prefix:     p.prefix,
		}
	}
	return res, nil
}

func (p *simpleExpressionPlanner) planEval() (shared.SQLRequestPlanner, error) {
	err := p.check()
	if err != nil {
		return nil, err
	}

	p.analyze()
	var res shared.SQLRequestPlanner
	if p.selector.Filter != nil {
		res = &AttrConditionEvaluatorPlanner{
			Main: &AttrConditionPlanner{
				Main:           NewInitIndexPlanner(true),
				Terms:          p.termIdx,
				Conds:          p.cond,
				AggregatedAttr: p.aggAttr,
			},
			Prefix: p.prefix,
		}
	} else {
		res = &AttrlessEvaluatorPlanner{
			Prefix: p.prefix,
		}
	}

	return res, nil
}

func (p *simpleExpressionPlanner) check() error {
	if p.selector.Filter == nil && p.selector.Aggregator != nil {
		return errEmptyAggregated
	}
	return nil
}

func (p *simpleExpressionPlanner) analyze() {
	p.terms = make(map[string]int)
	p.cond = p.analyzeCond(p.selector.Filter)
	if p.selector.Aggregator != nil {
		p.aggAttr = p.selector.Aggregator.Attr
	}
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
