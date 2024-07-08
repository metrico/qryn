package planners

import (
	"fmt"
	"wasm_parts/promql/shared"
	sql "wasm_parts/sql_select"
)

type LabelsInitPlanner struct {
	Main              shared.RequestPlanner
	FingerprintsAlias string
}

func (l *LabelsInitPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	main, err := l.Main.Process(ctx)
	if err != nil {
		return nil, err
	}

	var withFp *sql.With
	for _, w := range main.GetWith() {
		if w.GetAlias() == l.FingerprintsAlias {
			withFp = w
			break
		}
	}

	if withFp == nil {
		return nil, fmt.Errorf("fingerprints subrequest not found")
	}

	labelsCol := "mapFromArrays(" +
		"arrayMap(x -> x.1, JSONExtractKeysAndValues(time_series.labels, 'String') as ts_kv), " +
		"arrayMap(x -> x.2, ts_kv))"

	labelsSubSel := sql.NewSelect().Select(
		sql.NewSimpleCol("fingerprint", "fingerprint"),
		sql.NewSimpleCol(labelsCol, "labels"),
		sql.NewSimpleCol("fingerprint", "new_fingerprint")).
		From(sql.NewSimpleCol(ctx.TimeSeriesTable, "time_series")).
		AndWhere(
			sql.Ge(sql.NewRawObject("date"), sql.NewStringVal(ctx.From.Format("2006-01-02"))),
			sql.Le(sql.NewRawObject("date"), sql.NewStringVal(ctx.To.Format("2006-01-02"))),
			sql.NewIn(sql.NewRawObject("fingerprint"), sql.NewWithRef(withFp)))
	withLabelsSubSel := sql.NewWith(labelsSubSel, "labels")

	return main.AddWith(withLabelsSubSel), nil
}
