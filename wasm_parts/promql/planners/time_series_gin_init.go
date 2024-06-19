package planners

import (
	"wasm_parts/promql/shared"
	sql "wasm_parts/sql_select"
)

type TimeSeriesGinInitPlanner struct {
}

func (t *TimeSeriesGinInitPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	return sql.NewSelect().
		Select(sql.NewSimpleCol("fingerprint", "fingerprint")).
		From(sql.NewSimpleCol(ctx.TimeSeriesGinTable, "ts_gin")).
		AndWhere(
			sql.Ge(sql.NewRawObject("date"), sql.NewStringVal(ctx.From.Format("2006-01-02"))),
			sql.Le(sql.NewRawObject("date"), sql.NewStringVal(ctx.To.Format("2006-01-02"))),
			sql.NewIn(sql.NewRawObject("type"), sql.NewIntVal(0), sql.NewIntVal(2))).
		GroupBy(sql.NewRawObject("fingerprint")), nil
}
