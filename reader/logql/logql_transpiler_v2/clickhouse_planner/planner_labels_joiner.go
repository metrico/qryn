package clickhouse_planner

import (
	"github.com/metrico/qryn/reader/logql/logql_transpiler_v2/shared"
	sql "github.com/metrico/qryn/reader/utils/sql_select"
)

type LabelsJoinPlanner struct {
	Main         shared.SQLRequestPlanner
	Fingerprints shared.SQLRequestPlanner
	TimeSeries   shared.SQLRequestPlanner
	FpCache      **sql.With
	LabelsCache  **sql.With
}

func (l *LabelsJoinPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	tsReq, err := (&WithConnectorPlanner{
		Main:      l.TimeSeries,
		With:      l.Fingerprints,
		Alias:     "fp_sel",
		WithCache: l.FpCache,

		ProcessFn: func(q sql.ISelect, w *sql.With) (sql.ISelect, error) {
			return q.AndPreWhere(sql.NewIn(sql.NewRawObject("time_series.fingerprint"), sql.NewWithRef(w))), nil
		},
	}).Process(ctx)
	if err != nil {
		return nil, err
	}
	mainReq, err := l.Main.Process(ctx)
	if err != nil {
		return nil, err
	}
	withMain := sql.NewWith(mainReq, "main")
	withTS := sql.NewWith(tsReq, "_time_series")
	if l.LabelsCache != nil {
		*l.LabelsCache = withTS
	}

	joinType := "ANY LEFT "
	if ctx.IsCluster {
		joinType = "GLOBAL ANY LEFT "
	}

	return sql.NewSelect().
		With(withMain, withTS).
		Select(
			sql.NewSimpleCol("main.fingerprint", "fingerprint"),
			sql.NewSimpleCol("main.timestamp_ns", "timestamp_ns"),
			sql.NewSimpleCol("_time_series.labels", "labels"),
			sql.NewSimpleCol("main.string", "string"),
			sql.NewSimpleCol("main.value", "value")).
		From(sql.NewWithRef(withMain)).
		Join(sql.NewJoin(
			joinType,
			sql.NewWithRef(withTS),
			sql.Eq(sql.NewRawObject("main.fingerprint"), sql.NewRawObject("_time_series.fingerprint")))), nil
}
