package clickhouse_transpiler

import (
	"time"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	"github.com/metrico/qryn/v5/reader/traceql/timerange"
	traceql_ir "github.com/metrico/qryn/v5/reader/traceql/traceql_ir"
	sql "github.com/metrico/qryn/v5/reader/utils/sql_select"
)

// MetricsBuild builds a metrics query reading spans, a CTE with one row per
// span the metrics function counts: trace_id, span_id, timestamp_ns,
// duration_ns, name and service_name.
type MetricsBuild func(spans *sql.With) (sql.ISelect, error)

// CompareBuild builds compare() from two CTEs shaped like MetricsBuild's:
// baseline, the result spans over the baseline window, and selection, the
// spans of the selection filter over the query range, or nil for `{}`.
type CompareBuild func(baseline, selection *sql.With) (sql.ISelect, error)

// PlanMetrics plans a metrics query over the result spans of the query's
// spanset expression in the query range: the spans a search for it returns,
// uncapped.
func PlanMetrics(query *traceql_ir.Query, build MetricsBuild) (shared.SQLRequestPlanner, error) {
	p := &planner{query: query}
	spans, err := p.metricsSpans()
	if err != nil {
		return nil, err
	}
	return p.metricsQuery(&metricsPlanner{Spans: spans, Build: build}), nil
}

// PlanCompare plans compare(): the query's result spans over
// [baselineFrom, baselineTo] are the baseline, and the spans of the selection
// filter over the query range are the selection.
func PlanCompare(query *traceql_ir.Query, selection traceql_ir.FilterExpr,
	baselineFrom, baselineTo time.Time, build CompareBuild) (shared.SQLRequestPlanner, error) {
	p := &planner{query: query}
	baseline, err := p.metricsSpans()
	if err != nil {
		return nil, err
	}
	res := &comparePlanner{Baseline: baseline, BaselineFrom: baselineFrom, BaselineTo: baselineTo, Build: build}
	if selection != nil {
		op, err := p.operand(&traceql_ir.Selector{Filter: selection})
		if err != nil {
			return nil, err
		}
		filter, err := op.planner()
		if err != nil {
			return nil, err
		}
		res.Selection = &metricsSpansPlanner{Result: filter, Prefix: p.getPrefix()}
	}
	return p.metricsQuery(res), nil
}

// metricsSpans plans the metrics spans of the query's spanset expression.
func (p *planner) metricsSpans() (*metricsSpansPlanner, error) {
	expr, err := p.spansetExpr()
	if err != nil {
		return nil, err
	}
	result, err := p.planSpansetExpr(expr)
	if err != nil {
		return nil, err
	}
	return &metricsSpansPlanner{Result: result, Prefix: p.getPrefix()}, nil
}

// metricsQuery wraps res in StructuralQueryPlanner when the query has
// structural operators.
func (p *planner) metricsQuery(res shared.SQLRequestPlanner) shared.SQLRequestPlanner {
	if p.query.HasStructural() {
		return &StructuralQueryPlanner{Main: res}
	}
	return res
}

// metricsSpansPlanner reads the traces table for the result spans of Result,
// an evaluated spanset expression: one row per span, as MetricsBuild reads.
type metricsSpansPlanner struct {
	Result shared.SQLRequestPlanner
	Prefix string
}

func (m *metricsSpansPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	// Result's selectors set their traces-table conditions on a context of its own.
	resultCtx := *ctx
	resultCtx.TracesIntrinsicWhere = nil
	ctx = &resultCtx
	main, err := m.Result.Process(ctx)
	if err != nil {
		return nil, err
	}
	withResult := sql.NewWith(main, m.Prefix+"result_spans")
	ids := sql.NewSelect().
		Select(sql.NewRawObject("trace_id"), sql.NewRawObject("span_id")).
		From(sql.NewWithRef(withResult))
	if cond := intrinsicFilter(ctx, withResult); cond != nil {
		ids.AndWhere(cond)
	}
	withIds := sql.NewWith(ids, m.Prefix+"result_span_ids")
	tsFrom, tsTo := timerange.SpanStart("traces.timestamp_ns", ctx.From.UnixNano(), ctx.To.UnixNano())
	return sql.NewSelect().
		With(withResult, withIds).
		Select(
			sql.NewSimpleCol("traces.trace_id", "trace_id"),
			sql.NewSimpleCol("traces.span_id", "span_id"),
			sql.NewSimpleCol("any(traces.timestamp_ns)", "timestamp_ns"),
			sql.NewSimpleCol("any(traces.duration_ns)", "duration_ns"),
			sql.NewSimpleCol("any(traces.name)", "name"),
			sql.NewSimpleCol("any(traces.service_name)", "service_name")).
		From(sql.NewSimpleCol(tracesTable(ctx), "traces")).
		AndWhere(
			tsFrom,
			tsTo,
			inSubquery(ctx, sql.NewRawObject("traces.trace_id"),
				sql.NewSelect().Select(sql.NewRawObject("trace_id")).From(sql.NewWithRef(withIds))),
			inSubquery(ctx, sql.NewRawObject("(traces.trace_id, traces.span_id)"), sql.NewWithRef(withIds))).
		GroupBy(sql.NewRawObject("traces.trace_id"), sql.NewRawObject("traces.span_id")), nil
}

// metricsPlanner builds a metrics query over the metrics spans.
type metricsPlanner struct {
	Spans *metricsSpansPlanner
	Build MetricsBuild
}

func (m *metricsPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	spans, err := m.Spans.Process(ctx)
	if err != nil {
		return nil, err
	}
	return m.Build(sql.NewWith(spans, "metrics_spans"))
}

// comparePlanner builds compare() over the baseline spans, taken over the
// baseline window, and the selection spans, nil for `{}`.
type comparePlanner struct {
	Baseline                 *metricsSpansPlanner
	Selection                *metricsSpansPlanner
	BaselineFrom, BaselineTo time.Time
	Build                    CompareBuild
}

func (c *comparePlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	baselineCtx := *ctx
	baselineCtx.From, baselineCtx.To = c.BaselineFrom, c.BaselineTo
	baseline, err := c.Baseline.Process(&baselineCtx)
	if err != nil {
		return nil, err
	}
	var selection *sql.With
	if c.Selection != nil {
		spans, err := c.Selection.Process(ctx)
		if err != nil {
			return nil, err
		}
		selection = sql.NewWith(spans, "selection_spans")
	}
	return c.Build(sql.NewWith(baseline, "baseline_spans"), selection)
}
