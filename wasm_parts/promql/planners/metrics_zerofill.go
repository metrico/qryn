package planners

import (
	"fmt"
	"wasm_parts/promql/shared"
	sql "wasm_parts/sql_select"
)

type MetricsZeroFillPlanner struct {
	Main shared.RequestPlanner
}

func (m *MetricsZeroFillPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	main, err := m.Main.Process(ctx)
	if err != nil {
		return nil, err
	}
	main.OrderBy(sql.NewRawObject("fingerprint"), sql.NewCustomCol(func(_ *sql.Ctx, options ...int) (string, error) {
		return fmt.Sprintf("timestamp_ms WITH FILL FROM %d TO %d STEP %d",
			ctx.From.UnixMilli(), ctx.To.UnixMilli(), ctx.Step.Milliseconds()), nil
	}))
	return main, nil
	/*withMain := sql.NewWith(main, "prezerofill")
	arrLen := (ctx.To.UnixNano()-ctx.From.UnixNano())/ctx.Step.Nanoseconds() + 1
	zeroFillCol := sql.NewCustomCol(func(_ *sql.Ctx, options ...int) (string, error) {
		return fmt.Sprintf("groupArrayInsertAt(nan, %d)(value, toUInt32(intDiv(timestamp_ms - %d, %d)))",
			arrLen, ctx.From.UnixMilli(), ctx.Step.Milliseconds()), nil
	})
	zeroFill := sql.NewSelect().With(withMain).
		Select(
			sql.NewSimpleCol("fingerprint", "fingerprint"),
			sql.NewCol(zeroFillCol, "values")).
		From(sql.NewWithRef(withMain)).
		GroupBy(sql.NewRawObject("fingerprint"))
	withZeroFill := sql.NewWith(zeroFill, "zerofill")

	joinZeroFillStmt := sql.NewCustomCol(func(_ *sql.Ctx, options ...int) (string, error) {
		return fmt.Sprintf("arrayMap((x,y) -> (y * %d + %d, x), values, range(%d))",
			ctx.Step.Milliseconds(), ctx.From.UnixMilli(), arrLen), nil
	})

	postZeroFill := sql.NewSelect().With(withZeroFill).
		Select(
			sql.NewSimpleCol("fingerprint", "fingerprint"),
			sql.NewSimpleCol("timestamp_ms", "timestamp_ms"),
			sql.NewSimpleCol("val.2", "value")).
		From(sql.NewWithRef(withMain)).
		Join(sql.NewJoin("array", sql.NewCol(joinZeroFillStmt, "val"), nil))
	return postZeroFill, nil*/
}
