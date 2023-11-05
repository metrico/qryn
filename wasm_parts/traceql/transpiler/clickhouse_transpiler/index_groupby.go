package clickhouse_transpiler

import (
	sql "wasm_parts/sql_select"
	"wasm_parts/traceql/shared"
)

type IndexGroupByPlanner struct {
	Main shared.SQLRequestPlanner
}

func (i *IndexGroupByPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	main, err := i.Main.Process(ctx)
	if err != nil {
		return nil, err
	}

	withMain := sql.NewWith(main, "index_search")
	return sql.NewSelect().
		With(withMain).
		Select(
			sql.NewSimpleCol("trace_id", "trace_id"),
			sql.NewSimpleCol("groupArray(span_id)", "span_id"),
			sql.NewSimpleCol("groupArray(duration)", "duration"),
			sql.NewSimpleCol("groupArray(timestamp_ns)", "timestamp_ns")).
		From(sql.NewWithRef(withMain)).
		GroupBy(sql.NewRawObject("trace_id")).
		OrderBy(
			sql.NewOrderBy(sql.NewRawObject("max(index_search.timestamp_ns)"), sql.ORDER_BY_DIRECTION_DESC),
		), nil
}
