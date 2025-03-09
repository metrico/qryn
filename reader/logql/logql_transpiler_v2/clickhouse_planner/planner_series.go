package clickhouse_planner

import (
	"github.com/metrico/qryn/reader/logql/logql_transpiler_v2/shared"
	"github.com/metrico/qryn/reader/plugins"
	sql "github.com/metrico/qryn/reader/utils/sql_select"
)

type SeriesPlanner struct {
	FingerprintsPlanner shared.SQLRequestPlanner
}

func NewSeriesPlanner(fingerprintsPlanner shared.SQLRequestPlanner) shared.SQLRequestPlanner {
	p := plugins.GetSeriesPlannerPlugin()
	if p != nil {
		return (*p)(fingerprintsPlanner)
	}
	return &SeriesPlanner{FingerprintsPlanner: fingerprintsPlanner}
}

func (s *SeriesPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	fpSel, err := s.FingerprintsPlanner.Process(ctx)
	if err != nil {
		return nil, err
	}
	withFPSel := sql.NewWith(fpSel, "fp_sel")
	tableName := ctx.TimeSeriesTableName
	if ctx.IsCluster {
		tableName = ctx.TimeSeriesDistTableName
	}
	req := sql.NewSelect().With(withFPSel).Distinct(true).
		Select(sql.NewSimpleCol("labels", "labels")).
		From(sql.NewSimpleCol(tableName, "time_series")).
		AndWhere(
			sql.Ge(sql.NewRawObject("date"), sql.NewStringVal(FormatFromDate(ctx.From))),
			sql.Le(sql.NewRawObject("date"), sql.NewStringVal(ctx.To.Format("2006-01-02"))),
			sql.NewIn(sql.NewRawObject("fingerprint"), sql.NewWithRef(withFPSel)),
			GetTypes(ctx))
	if ctx.Limit > 0 {
		req.Limit(sql.NewIntVal(ctx.Limit))
	}
	return req, nil
}
