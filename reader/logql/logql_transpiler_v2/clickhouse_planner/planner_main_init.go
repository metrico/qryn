package clickhouse_planner

import (
	"github.com/metrico/qryn/reader/logql/logql_transpiler_v2/shared"
	"github.com/metrico/qryn/reader/plugins"
	sql "github.com/metrico/qryn/reader/utils/sql_select"
)

type SqlMainInitPlanner struct {
}

func NewSQLMainInitPlanner() shared.SQLRequestPlanner {
	p := plugins.GetSqlMainInitPlannerPlugin()
	if p != nil {
		return (*p)()
	}
	return &SqlMainInitPlanner{}
}

func (s *SqlMainInitPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	return sql.NewSelect().
		Select(
			sql.NewSimpleCol("samples.timestamp_ns", "timestamp_ns"),
			sql.NewSimpleCol("samples.fingerprint", "fingerprint"),
			sql.NewSimpleCol("samples.string", "string"),
			sql.NewSimpleCol("toFloat64(0)", "value"),
		).From(sql.NewSimpleCol(ctx.SamplesTableName, "samples")).
		AndPreWhere(
			sql.Ge(sql.NewRawObject("samples.timestamp_ns"), sql.NewIntVal(ctx.From.UnixNano())),
			sql.Lt(sql.NewRawObject("samples.timestamp_ns"), sql.NewIntVal(ctx.To.UnixNano())),
			GetTypes(ctx)), nil
}
