package clickhouse_transpiler

import (
	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	sql "github.com/metrico/qryn/v5/reader/utils/sql_select"
)

// SelectValuesRequestPlanner reads the values of attribute Key of the result
// spans of Result, an evaluated spanset expression.
type SelectValuesRequestPlanner struct {
	Result shared.SQLRequestPlanner
	Prefix string
	Key    string
}

func (s *SelectValuesRequestPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	return selectAttrs(ctx, s.Result, s.Prefix, "val", sql.Eq(sql.NewRawObject("key"), sql.NewStringVal(s.Key)))
}
