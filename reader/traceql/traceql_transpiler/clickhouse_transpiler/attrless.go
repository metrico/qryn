package clickhouse_transpiler

import (
	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	"github.com/metrico/qryn/v5/reader/plugins"
	"github.com/metrico/qryn/v5/reader/traceql/timerange"
	sql "github.com/metrico/qryn/v5/reader/utils/sql_select"
)

type AttrlessConditionPlanner struct {
}

func NewAttrlessConditionPlanner() shared.SQLRequestPlanner {
	p := plugins.GetAttrlessConditionPlannerPlugin()
	if p != nil {
		return (*p)()
	}
	return &AttrlessConditionPlanner{}
}

func (a *AttrlessConditionPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	tracesTable := ctx.TracesTable
	from, to := ctx.From.UnixNano(), ctx.To.UnixNano()
	pickFrom, pickTo := timerange.TracePick("timestamp_ns", from, to)
	tsFrom, tsTo := timerange.SpanStart("timestamp_ns", from, to)
	traceIds := sql.NewSelect().Select(sql.NewSimpleCol("trace_id", "trace_id")).
		Distinct(true).
		From(sql.NewSimpleCol(tracesTable, "traces")).
		AndWhere(sql.And(
			pickFrom,
			pickTo,
		)).OrderBy(sql.NewOrderBy(sql.NewRawObject("timestamp_ns"), sql.ORDER_BY_DIRECTION_DESC)).
		Limit(sql.NewIntVal(ctx.Limit))
	withTraceIds := sql.NewWith(traceIds, "trace_ids")
	traceAndSpanIds := sql.NewSelect().
		Select(
			sql.NewSimpleCol("trace_id", "trace_id"),
			sql.NewSimpleCol("groupArray(100)(span_id)", "span_id")).
		From(sql.NewSimpleCol(tracesTable, "traces")).
		AndWhere(sql.And(
			tsFrom,
			tsTo,
			sql.NewIn(sql.NewRawObject("trace_id"), sql.NewWithRef(withTraceIds)),
		)).
		GroupBy(sql.NewRawObject("trace_id"))
	withTraceAndSpanIds := sql.NewWith(traceAndSpanIds, "trace_and_span_ids")
	traceAndSpanIdsUnnested := sql.NewSelect().
		Select(
			sql.NewSimpleCol("trace_id", "trace_id"),
			sql.NewSimpleCol("_span_id", "span_id")).
		From(sql.NewWithRef(withTraceAndSpanIds)).
		Join(sql.NewJoin("array", sql.NewSimpleCol(withTraceAndSpanIds.GetAlias()+".span_id", "_span_id"), nil))
	withTraceAndSpanIdsUnnested := sql.NewWith(traceAndSpanIdsUnnested, "trace_and_span_ids_unnested")
	return sql.NewSelect().
		With(withTraceIds, withTraceAndSpanIds, withTraceAndSpanIdsUnnested).
		Select(
			sql.NewSimpleCol("trace_id", "trace_id"),
			sql.NewSimpleCol("span_id", "span_id"),
			sql.NewSimpleCol("duration_ns", "duration"),
			sql.NewSimpleCol("timestamp_ns", "timestamp_ns")).
		From(sql.NewSimpleCol(tracesTable, "traces")).
		AndWhere(sql.And(
			tsFrom,
			tsTo,
			sql.NewIn(
				sql.NewRawObject("(traces.trace_id, traces.span_id)"),
				sql.NewWithRef(withTraceAndSpanIdsUnnested)))).
		OrderBy(sql.NewOrderBy(sql.NewRawObject("timestamp_ns"), sql.ORDER_BY_DIRECTION_DESC)), nil
}
