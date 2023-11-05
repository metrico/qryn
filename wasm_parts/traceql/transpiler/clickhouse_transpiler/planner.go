package clickhouse_transpiler

import (
	traceql_parser "wasm_parts/traceql/parser"
	"wasm_parts/traceql/shared"
)

func Plan(script *traceql_parser.TraceQLScript) (shared.SQLRequestPlanner, error) {
	return (&planner{script: script}).plan()
}

type planner struct {
	script *traceql_parser.TraceQLScript

	//Analyze results
	termIdx []*traceql_parser.AttrSelector
	cond    *condition
	aggFn   string
	aggAttr string
	cmpVal  string

	terms map[string]int
}

func (p *planner) plan() (shared.SQLRequestPlanner, error) {
	err := p.check()
	if err != nil {
		return nil, err
	}

	p.analyze()

	var res shared.SQLRequestPlanner = &AttrConditionPlanner{
		Main:           &InitIndexPlanner{},
		Terms:          p.termIdx,
		Conds:          p.cond,
		AggregatedAttr: p.aggAttr,
	}

	res = &IndexGroupByPlanner{res}

	if p.aggFn != "" {
		res = &AggregatorPlanner{
			Main:       res,
			Fn:         p.aggFn,
			Attr:       p.aggAttr,
			CompareFn:  p.script.Head.Aggregator.Cmp,
			CompareVal: p.script.Head.Aggregator.Num + p.script.Head.Aggregator.Measurement,
		}
	}

	res = &IndexLimitPlanner{res}

	res = &TracesDataPlanner{Main: res}

	res = &IndexLimitPlanner{res}

	return res, nil
}

func (p *planner) check() error {
	if p.script.Tail != nil {
		return &shared.NotSupportedError{Msg: "more than one selector not supported"}
	}
	return nil
}

func (p *planner) analyze() {
	p.terms = make(map[string]int)
	p.cond = p.analyzeCond(&p.script.Head.AttrSelector)
	p.analyzeAgg()
}

func (p *planner) analyzeCond(exp *traceql_parser.AttrSelectorExp) *condition {
	var res *condition
	if exp.ComplexHead != nil {
		res = p.analyzeCond(exp.ComplexHead)
	} else if exp.Head != nil {
		term := exp.Head.String()
		if p.terms[term] != 0 {
			res = &condition{simpleIdx: p.terms[term] - 1}
		} else {
			p.termIdx = append(p.termIdx, exp.Head)
			p.terms[term] = len(p.termIdx)
			res = &condition{simpleIdx: len(p.termIdx) - 1}
		}
	}
	if exp.Tail != nil {
		res = &condition{
			simpleIdx: -1,
			op:        exp.AndOr,
			complex:   []*condition{res, p.analyzeCond(exp.Tail)},
		}
	}
	return res
}

func (p *planner) analyzeAgg() {
	if p.script.Head.Aggregator == nil {
		return
	}

	p.aggFn = p.script.Head.Aggregator.Fn
	p.aggAttr = p.script.Head.Aggregator.Attr

	p.cmpVal = p.script.Head.Aggregator.Num + p.script.Head.Aggregator.Measurement
	return
}

type condition struct {
	simpleIdx int // index of term; -1 means complex

	op      string
	complex []*condition
}
