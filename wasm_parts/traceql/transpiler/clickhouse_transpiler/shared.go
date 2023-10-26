package clickhouse_transpiler

import (
	sql "wasm_parts/sql_select"
	"wasm_parts/traceql/shared"
)

func getComparisonFn(op string) (func(left sql.SQLObject, right sql.SQLObject) *sql.LogicalOp, error) {
	switch op {
	case "=":
		return sql.Eq, nil
	case ">":
		return sql.Gt, nil
	case "<":
		return sql.Lt, nil
	case ">=":
		return sql.Ge, nil
	case "<=":
		return sql.Le, nil
	case "!=":
		return sql.Neq, nil
	}
	return nil, &shared.NotSupportedError{Msg: "not supported operator: " + op}
}
