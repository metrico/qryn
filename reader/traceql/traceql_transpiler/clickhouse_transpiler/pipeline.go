package clickhouse_transpiler

import (
	"fmt"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	traceql_ir "github.com/metrico/qryn/v5/reader/traceql/traceql_ir"
	sql "github.com/metrico/qryn/v5/reader/utils/sql_select"
)

// emptyAttrs is the spanset attributes column of a stage that sets none:
// (name, type, value) per aggregate, type one of AttrInt, AttrFloat and AttrDuration.
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

// readsEverySpan reports whether a pipeline starts with a scalar filter, which
// reads every span in range.
func readsEverySpan(pipeline *traceql_ir.Pipeline) bool {
	_, ok := pipeline.Elements[0].(*traceql_ir.ScalarFilter)
	return ok
}

// planPipeline plans a pipeline into its result span rows.
func (p *planner) planPipeline(pipeline *traceql_ir.Pipeline) (shared.SQLRequestPlanner, error) {
	var stages []pipelineStage
	if readsEverySpan(pipeline) {
		stages = append(stages, &spansetStage{Expr: &AllSpansPlanner{}})
	}
	for _, element := range pipeline.Elements {
		switch e := element.(type) {
		case *traceql_ir.ScalarFilter:
			stage, err := newScalarFilterStage(e)
			if err != nil {
				return nil, err
			}
			stages = append(stages, stage)
		case traceql_ir.SpansetExpr:
			expr, err := p.planSpansetExpr(e)
			if err != nil {
				return nil, err
			}
			stages = append(stages, &spansetStage{Expr: expr, OwnAttrs: isPipeline(e)})
		}
	}
	return &pipelinePlanner{Stages: stages, Prefix: p.getPrefix()}, nil
}

// isPipeline reports whether expr is a parenthesised pipeline.
func isPipeline(expr traceql_ir.SpansetExpr) bool {
	for {
		switch e := expr.(type) {
		case *traceql_ir.Group:
			expr = e.Expr
		case *traceql_ir.Pipeline:
			return true
		default:
			return false
		}
	}
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
// traces-table conditions of its selectors. After another stage, its
// selectors match only that stage's spans, whose attributes it keeps.
type spansetStage struct {
	Expr shared.SQLRequestPlanner
	// OwnAttrs is set when Expr's rows carry an attrs column, appended to
	// the kept attributes.
	OwnAttrs bool
}

func (s *spansetStage) process(ctx *shared.PlannerContext, alias string, prev, _ *sql.With) (sql.ISelect, error) {
	// The selectors set their traces-table conditions on a context of their own.
	exprCtx := *ctx
	exprCtx.TracesIntrinsicWhere = nil
	if prev != nil {
		exprCtx.TracesOperandSpans = prev
	}
	main, err := s.Expr.Process(&exprCtx)
	if err != nil {
		return nil, err
	}
	withExpr := sql.NewWith(main, alias+"_expr")
	e := withExpr.GetAlias()
	attrs := emptyAttrs
	if prev != nil || s.OwnAttrs {
		kept, own := emptyAttrs, emptyAttrs
		if prev != nil {
			kept = prev.GetAlias() + ".attrs"
		}
		if s.OwnAttrs {
			own = e + ".attrs"
		}
		attrs = fmt.Sprintf("arrayConcat(%s, %s)", kept, own)
	}
	res := sql.NewSelect().
		With(withExpr).
		Select(append(spanRowColsOf(e), sql.NewSimpleCol(attrs, "attrs"))...).
		From(sql.NewWithRef(withExpr))
	if prev != nil {
		res.Join(sql.NewJoin("inner", sql.NewWithRef(prev), sameSpan(prev.GetAlias()+".span_id", e+".span_id")))
	}
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
