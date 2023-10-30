package traceql_transpiler

import (
	traceql_parser "wasm_parts/traceql/parser"
	"wasm_parts/traceql/shared"
	"wasm_parts/traceql/transpiler/clickhouse_transpiler"
)

func Plan(script *traceql_parser.TraceQLScript) (shared.SQLRequestPlanner, error) {
	sqlPlanner, err := clickhouse_transpiler.Plan(script)
	if err != nil {
		return nil, err
	}
	return sqlPlanner, nil
}
