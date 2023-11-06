package clickhouse_transpiler

import (
	sql "wasm_parts/sql_select"
	"wasm_parts/traceql/shared"
)

type InitIndexPlanner struct {
}

func (i *InitIndexPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	return sql.NewSelect().Select(
		sql.NewSimpleCol("trace_id", "trace_id"),
		sql.NewSimpleCol("lower(hex(span_id))", "span_id"),
		sql.NewSimpleCol("any(duration)", "duration"),
		sql.NewSimpleCol("any(timestamp_ns)", "timestamp_ns")).
		From(sql.NewSimpleCol(ctx.TracesAttrsTable, "traces_idx")).
		AndWhere(sql.And(
			sql.Eq(sql.NewRawObject("oid"), sql.NewStringVal(ctx.OrgID)),
			sql.Ge(sql.NewRawObject("date"), sql.NewStringVal(ctx.From.Format("2006-01-02"))),
			sql.Le(sql.NewRawObject("date"), sql.NewStringVal(ctx.To.Format("2006-01-02"))),
			sql.Ge(sql.NewRawObject("traces_idx.timestamp_ns"), sql.NewIntVal(ctx.From.UnixNano())),
			sql.Lt(sql.NewRawObject("traces_idx.timestamp_ns"), sql.NewIntVal(ctx.To.UnixNano())),
		)).GroupBy(sql.NewRawObject("trace_id"), sql.NewRawObject("span_id")).
		OrderBy(sql.NewOrderBy(sql.NewRawObject("timestamp_ns"), sql.ORDER_BY_DIRECTION_DESC)), nil
}
