package planners

import (
	"fmt"
	"wasm_parts/promql/shared"
	sql "wasm_parts/sql_select"
)

type MetricsExtendPlanner struct {
	Main shared.RequestPlanner
}

func (m *MetricsExtendPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	main, err := m.Main.Process(ctx)
	if err != nil {
		return nil, err
	}
	extendCnt := 300000 / ctx.Step.Milliseconds()
	if extendCnt < 1 {
		return main, nil
	}
	withMain := sql.NewWith(main, "pre_extend")
	extendedCol := sql.NewCustomCol(func(ctx *sql.Ctx, options ...int) (string, error) {
		return fmt.Sprintf(
			"argMaxIf(value, timestamp_ms, pre_extend.original = 1) OVER ("+
				"PARTITION BY fingerprint ORDER BY timestamp_ms ROWS BETWEEN %d PRECEDING AND CURRENT ROW"+
				")", extendCnt), nil
	})
	origCol := sql.NewCustomCol(func(ctx *sql.Ctx, options ...int) (string, error) {
		return fmt.Sprintf(
			"max(original) OVER ("+
				"PARTITION BY fingerprint ORDER BY timestamp_ms ROWS BETWEEN %d PRECEDING AND CURRENT ROW"+
				")", extendCnt), nil
	})
	extend := sql.NewSelect().With(withMain).
		Select(
			sql.NewSimpleCol("fingerprint", "fingerprint"),
			sql.NewSimpleCol("timestamp_ms", "timestamp_ms"),
			sql.NewCol(extendedCol, "value"),
			sql.NewCol(origCol, "original")).
		From(sql.NewWithRef(withMain))
	withExtend := sql.NewWith(extend, "extend")
	return sql.NewSelect().With(withExtend).Select(sql.NewRawObject("*")).
		From(sql.NewWithRef(withExtend)).
		AndWhere(sql.Eq(sql.NewRawObject("original"), sql.NewIntVal(1))), nil
}
