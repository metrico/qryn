package clickhouse_transpiler

import (
	sql "wasm_parts/sql_select"
	"wasm_parts/traceql/shared"
)

type IndexLimitPlanner struct {
	Main shared.SQLRequestPlanner
}

func (i *IndexLimitPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	main, err := i.Main.Process(ctx)
	if err != nil {
		return nil, err
	}

	if ctx.Limit == 0 {
		return main, nil
	}

	return main.Limit(sql.NewIntVal(ctx.Limit)), nil
}
