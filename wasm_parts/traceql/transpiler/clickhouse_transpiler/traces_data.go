package clickhouse_transpiler

import (
	sql "wasm_parts/sql_select"
	"wasm_parts/traceql/shared"
)

type TracesDataPlanner struct {
	Main shared.SQLRequestPlanner
}

func (t *TracesDataPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	main, err := t.Main.Process(ctx)
	if err != nil {
		return nil, err
	}

	table := ctx.TracesTable
	if ctx.IsCluster {
		table = ctx.TracesDistTable
	}

	withMain := sql.NewWith(main, "index_grouped")
	withTraceIds := sql.NewWith(
		sql.NewSelect().Select(sql.NewRawObject("trace_id")).From(sql.NewWithRef(withMain)),
		"trace_ids")
	return sql.NewSelect().
		With(withMain, withTraceIds).
		Select(
			sql.NewSimpleCol("lower(hex(traces.trace_id))", "trace_id"),
			sql.NewSimpleCol("any(index_grouped.span_id)", "span_id"),
			sql.NewSimpleCol("any(index_grouped.duration)", "duration"),
			sql.NewSimpleCol("any(index_grouped.timestamp_ns)", "timestamps_ns"),
			sql.NewSimpleCol("min(traces.timestamp_ns)", "start_time_unix_nano"),
			sql.NewSimpleCol(
				"toFloat64(max(traces.timestamp_ns + traces.duration_ns) - min(traces.timestamp_ns)) / 1000000",
				"duration_ms"),
			sql.NewSimpleCol("argMin(traces.service_name, traces.timestamp_ns)", "root_service_name"),
			sql.NewSimpleCol("argMin(traces.name, traces.timestamp_ns)", "root_trace_name"),
		).
		From(sql.NewSimpleCol(table, "traces")).
		Join(sql.NewJoin("LEFT ANY",
			sql.NewWithRef(withMain),
			sql.Eq(sql.NewRawObject("traces.trace_id"), sql.NewRawObject("index_grouped.trace_id")))).
		AndWhere(
			sql.Eq(sql.NewRawObject("oid"), sql.NewStringVal(ctx.OrgID)),
			sql.NewIn(sql.NewRawObject("traces.trace_id"), sql.NewWithRef(withTraceIds))).
		GroupBy(sql.NewRawObject("traces.trace_id")).
		OrderBy(sql.NewOrderBy(sql.NewRawObject("start_time_unix_nano"), sql.ORDER_BY_DIRECTION_DESC)), nil
}
