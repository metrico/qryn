package planners

import (
	"fmt"
	"wasm_parts/promql/shared"
	sql "wasm_parts/sql_select"
)

type MetricsInitPlanner struct {
	ValueCol    sql.SQLObject
	Fingerprint shared.RequestPlanner
}

func (m *MetricsInitPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	fpReq, err := m.Fingerprint.Process(ctx)
	if err != nil {
		return nil, err
	}
	withFpReq := sql.NewWith(fpReq, "fp_sel")
	if m.ValueCol == nil {
		m.ValueCol = sql.NewRawObject("argMaxMerge(last)")
	}
	tsNsCol := sql.NewCustomCol(func(_ *sql.Ctx, options ...int) (string, error) {
		return fmt.Sprintf("intDiv(timestamp_ns, %d) * %d", ctx.Step.Nanoseconds(), ctx.Step.Milliseconds()), nil
	})
	return sql.NewSelect().With(withFpReq).Select(
		sql.NewSimpleCol("fingerprint", "fingerprint"),
		sql.NewCol(tsNsCol, "timestamp_ms"),
		sql.NewCol(m.ValueCol, "value")).
		From(sql.NewSimpleCol(ctx.MetricsTable, "metrics")).
		AndWhere(
			sql.Ge(sql.NewRawObject("timestamp_ns"), sql.NewIntVal(ctx.From.UnixNano())),
			sql.Le(sql.NewRawObject("timestamp_ns"), sql.NewIntVal(ctx.To.UnixNano())),
			sql.NewIn(sql.NewRawObject("fingerprint"), sql.NewWithRef(withFpReq))).
		GroupBy(sql.NewRawObject("fingerprint"), sql.NewRawObject("timestamp_ms")), nil
}
