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
			"argMaxIf(value, timestamp_ms, isNaN(value) = 0) OVER ("+
				"PARTITION BY fingerprint ORDER BY timestamp_ms ROWS BETWEEN %d PRECEDING AND CURRENT ROW"+
				")", extendCnt), nil
	})
	extend := sql.NewSelect().With(withMain).
		Select(
			sql.NewSimpleCol("fingerprint", "fingerprint"),
			sql.NewSimpleCol("timestamp_ms", "timestamp_ms"),
			sql.NewCol(extendedCol, "value")).
		From(sql.NewWithRef(withMain))
	return extend, nil
}
