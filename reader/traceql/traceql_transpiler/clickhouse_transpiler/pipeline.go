package clickhouse_transpiler

import (
	"fmt"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	traceql_ir "github.com/metrico/qryn/v5/reader/traceql/traceql_ir"
	sql "github.com/metrico/qryn/v5/reader/utils/sql_select"
)

// emptyAttrs is the spanset attributes column of a stage that sets none:
// (name, type, value) per aggregate, type 'int', 'float' or 'duration'.
const emptyAttrs = "CAST([], 'Array(Tuple(String, String, Float64))')"

// planResult plans the result span rows of the query's pipeline. With
// prepick, a lone `{}` reads only the ctx.Limit traces with the newest spans.
func (p *planner) planResult(prepick bool) (shared.SQLRequestPlanner, error) {
	if prepick && AllSpans(p.query) {
		return &pipelinePlanner{
			Stages: []pipelineStage{&spansetStage{Expr: NewAttrlessConditionPlanner()}},
			Prefix: p.getPrefix(),
		}, nil
	}
	return p.planPipeline(&p.query.Pipeline)
}

// planPipeline plans a pipeline into its result span rows. A leading scalar
// filter reads every span in range.
func (p *planner) planPipeline(pipeline *traceql_ir.Pipeline) (shared.SQLRequestPlanner, error) {
	var stages []pipelineStage
	for i, element := range pipeline.Elements {
		switch e := element.(type) {
		case *traceql_ir.ScalarFilter:
			stage, err := newScalarFilterStage(e)
			if err != nil {
				return nil, err
			}
			if i == 0 {
				stages = append(stages, &spansetStage{Expr: &AllSpansPlanner{}})
			}
			stages = append(stages, stage)
		case traceql_ir.SpansetExpr:
			if i > 0 {
				return nil, &shared.NotSupportedError{Msg: "a spanset filter after a pipeline stage is not supported"}
			}
			expr, err := p.planSpansetExpr(e)
			if err != nil {
				return nil, err
			}
			stages = append(stages, &spansetStage{Expr: expr})
		}
	}
	return &pipelinePlanner{Stages: stages, Prefix: p.getPrefix()}, nil
}

// pipelineStage plans one stage over prev, the previous stage's rows, and
// base, the rows of the last spanset stage before it. Both are nil for the
// first stage.
type pipelineStage interface {
	process(ctx *shared.PlannerContext, alias string, prev, base *sql.With) (sql.ISelect, error)
}

// pipelinePlanner evaluates its stages in order, one CTE each, into span
// rows: trace_id, span_id, timestamp_ns and attrs, the spanset attributes.
type pipelinePlanner struct {
	Stages []pipelineStage
	Prefix string
}

func (p *pipelinePlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	var prev, base *sql.With
	withs := make([]*sql.With, len(p.Stages))
	for i, stage := range p.Stages {
		alias := fmt.Sprintf("%sstage%d", p.Prefix, i)
		sel, err := stage.process(ctx, alias, prev, base)
		if err != nil {
			return nil, err
		}
		withs[i] = sql.NewWith(sel, alias)
		prev = withs[i]
		if _, ok := stage.(*spansetStage); ok {
			base = prev
		}
	}
	return sql.NewSelect().
		With(withs...).
		Select(append(spanRowCols(), sql.NewSimpleCol("attrs", "attrs"))...).
		From(sql.NewWithRef(prev)), nil
}

// spansetStage is the span rows of a spanset expression that pass the
// traces-table conditions of its selectors.
type spansetStage struct {
	Expr shared.SQLRequestPlanner
}

func (s *spansetStage) process(ctx *shared.PlannerContext, alias string, _, _ *sql.With) (sql.ISelect, error) {
	// The selectors set their traces-table conditions on a context of their own.
	exprCtx := *ctx
	exprCtx.TracesIntrinsicWhere = nil
	main, err := s.Expr.Process(&exprCtx)
	if err != nil {
		return nil, err
	}
	withExpr := sql.NewWith(main, alias+"_expr")
	res := sql.NewSelect().
		With(withExpr).
		Select(append(spanRowCols(), sql.NewSimpleCol(emptyAttrs, "attrs"))...).
		From(sql.NewWithRef(withExpr))
	if cond := intrinsicFilter(&exprCtx, withExpr); cond != nil {
		res.AndWhere(cond)
	}
	return res, nil
}

// intrinsicFilter keeps the rows of spans, evaluated span rows, that pass the
// traces-table conditions the expression's selectors set, or is nil without any.
func intrinsicFilter(ctx *shared.PlannerContext, spans *sql.With) sql.SQLCondition {
	if len(ctx.TracesIntrinsicWhere) == 0 {
		return nil
	}
	intrinsic := sql.NewSelect().
		Select(sql.NewRawObject("traces.trace_id"), sql.NewRawObject("traces.span_id")).
		From(sql.NewSimpleCol(tracesTable(ctx), "traces")).
		AndWhere(inSubquery(ctx, sql.NewRawObject("traces.trace_id"),
			sql.NewSelect().Select(sql.NewRawObject("trace_id")).From(sql.NewWithRef(spans)))).
		AndWhere(ctx.TracesIntrinsicWhere...)
	alias := spans.GetAlias()
	return sql.NewIn(sql.NewRawObject(fmt.Sprintf("(%s.trace_id, %s.span_id)", alias, alias)), intrinsic)
}
