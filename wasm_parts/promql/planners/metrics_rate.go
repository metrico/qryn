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
	resetCol := sql.NewCustomCol(func(ctx *sql.Ctx, options ...int) (string, error) {
		return fmt.Sprintf(
			"if(value < (any(value) OVER (" +
				"PARTITION BY fingerprint ORDER BY timestamp_ms ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING" +
				") as lastValue), lastValue, 0)"), nil
	})
	reset := sql.NewSelect().With(withMain).
		Select(
			sql.NewSimpleCol("fingerprint", "fingerprint"),
			sql.NewSimpleCol("timestamp_ms", "timestamp_ms"),
			sql.NewCol(resetCol, "reset"),
			sql.NewSimpleCol("value", "value")).
		From(sql.NewWithRef(withMain))
	withReset := sql.NewWith(reset, "pre_reset")
	resetColSum := sql.NewCustomCol(func(ctx *sql.Ctx, options ...int) (string, error) {
		_rateCnt := rateCnt - 1
		if rateCnt <= 1 {
			_rateCnt = 1
		}
		return fmt.Sprintf(
			"sum(reset) OVER ("+
				"PARTITION BY fingerprint ORDER BY timestamp_ms ROWS BETWEEN %d PRECEDING AND CURRENT ROW"+
				")", _rateCnt), nil
	})
	extend := sql.NewSelect().With(withReset).
		Select(
			sql.NewSimpleCol("fingerprint", "fingerprint"),
			sql.NewSimpleCol("timestamp_ms", "timestamp_ms"),
			sql.NewCol(lastCol, "last"),
			sql.NewCol(firstCol, "first"),
			sql.NewCol(resetColSum, "reset"),
			sql.NewSimpleCol(fmt.Sprintf("(last - first + reset) / %f", m.Duration.Seconds()), "_value")).
		From(sql.NewWithRef(withReset))
	withExtend := sql.NewWith(extend, "rate")
	return sql.NewSelect().
		With(withExtend).
		Select(sql.NewSimpleCol("fingerprint", "fingerprint"),
			sql.NewSimpleCol("timestamp_ms", "timestamp_ms"),
			sql.NewSimpleCol("_value", "value")).
		From(sql.NewWithRef(withExtend)), nil
}
