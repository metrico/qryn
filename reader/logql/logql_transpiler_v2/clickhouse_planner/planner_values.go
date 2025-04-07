package clickhouse_planner

import (
	"github.com/metrico/qryn/reader/logql/logql_transpiler_v2/shared"
	"github.com/metrico/qryn/reader/plugins"
	sql "github.com/metrico/qryn/reader/utils/sql_select"
)

type ValuesPlanner struct {
	FingerprintsPlanner shared.SQLRequestPlanner
	Key                 string
}

func NewValuesPlanner(fingerprintsPlanner shared.SQLRequestPlanner, key string) shared.SQLRequestPlanner {
	p := plugins.GetValuesPlannerPlugin()
	if p != nil {
		return (*p)(fingerprintsPlanner, key)
	}
	return &ValuesPlanner{FingerprintsPlanner: fingerprintsPlanner, Key: key}
}

func (v *ValuesPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	res := sql.NewSelect().Select(sql.NewRawObject("val")).Distinct(true).
		From(sql.NewRawObject(ctx.TimeSeriesGinTableName)).
		AndWhere(
			sql.Ge(sql.NewRawObject("date"), sql.NewStringVal(FormatFromDate(ctx.From))),
			sql.Le(sql.NewRawObject("date"), sql.NewStringVal(ctx.To.Format("2006-01-02"))),
			sql.Eq(sql.NewRawObject("key"), sql.NewStringVal(v.Key)),
			GetTypes(ctx),
		)
	if v.FingerprintsPlanner != nil {
		fp, err := v.FingerprintsPlanner.Process(ctx)
		if err != nil {
			return nil, err
		}

		withFp := sql.NewWith(fp, "fp_sel")
		res = res.With(withFp).
			AndWhere(sql.NewIn(sql.NewRawObject("fingerprint"), sql.NewWithRef(withFp)))
	}

	if ctx.Limit > 0 {
		res.Limit(sql.NewIntVal(ctx.Limit))
	}
	return res, nil
}
