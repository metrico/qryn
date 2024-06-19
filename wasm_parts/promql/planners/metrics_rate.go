package planners

import (
	"fmt"
	"time"
	"wasm_parts/promql/shared"
	sql "wasm_parts/sql_select"
)

type RatePlanner struct {
	Main     shared.RequestPlanner
	Duration time.Duration
}

func (m *RatePlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	main, err := m.Main.Process(ctx)
	if err != nil {
		return nil, err
	}
	rateCnt := m.Duration.Milliseconds() / ctx.Step.Milliseconds()
	if rateCnt < 1 {
		rateCnt = 1
	}
	withMain := sql.NewWith(main, "pre_rate")
	lastCol := sql.NewCustomCol(func(ctx *sql.Ctx, options ...int) (string, error) {
		return fmt.Sprintf(
			"argMax(value, timestamp_ms) OVER ("+
				"PARTITION BY fingerprint ORDER BY timestamp_ms ROWS BETWEEN %d PRECEDING AND CURRENT ROW"+
				")", rateCnt), nil
	})
	firstCol := sql.NewCustomCol(func(ctx *sql.Ctx, options ...int) (string, error) {
		return fmt.Sprintf(
			"argMin(value, timestamp_ms) OVER ("+
				"PARTITION BY fingerprint ORDER BY timestamp_ms ROWS BETWEEN %d PRECEDING AND CURRENT ROW"+
				")", rateCnt), nil
	})
	valueCol := sql.NewCustomCol(func(ctx *sql.Ctx, options ...int) (string, error) {
		return fmt.Sprintf(
			"if(last > first, last - first, last) / %f", m.Duration.Seconds()), nil
	})
	extend := sql.NewSelect().With(withMain).
		Select(
			sql.NewSimpleCol("fingerprint", "fingerprint"),
			sql.NewSimpleCol("timestamp_ms", "timestamp_ms"),
			sql.NewCol(lastCol, "last"),
			sql.NewCol(firstCol, "first"),
			sql.NewCol(valueCol, "_value")).
		From(sql.NewWithRef(withMain))
	withExtend := sql.NewWith(extend, "rate")
	return sql.NewSelect().
		With(withExtend).
		Select(sql.NewSimpleCol("fingerprint", "fingerprint"),
			sql.NewSimpleCol("timestamp_ms", "timestamp_ms"),
			sql.NewSimpleCol("_value", "value")).
		From(sql.NewWithRef(withExtend)), nil
}
