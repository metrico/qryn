package planners

import (
	"fmt"
	"wasm_parts/promql/shared"
	sql "wasm_parts/sql_select"
)

type FinalizePlanner struct {
	LabelsAlias string
	Main        shared.RequestPlanner
}

func (f *FinalizePlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	main, err := f.Main.Process(ctx)
	if err != nil {
		return nil, err
	}

	var withLabels *sql.With
	for _, w := range main.GetWith() {
		if w.GetAlias() == f.LabelsAlias {
			withLabels = w
			break
		}
	}

	if withLabels == nil {
		return nil, fmt.Errorf("FinalizePlanner.Process: %s CTE not found", f.LabelsAlias)
	}

	withMain := sql.NewWith(main, "pre_final")
	res := sql.NewSelect().With(withMain).Select(withMain).
		Select(
			sql.NewSimpleCol(withLabels.GetAlias()+".labels", "labels"),
			sql.NewSimpleCol("arraySort(groupArray((pre_final.timestamp_ms, pre_final.value)))", "values"),
		).From(sql.NewWithRef(withMain)).
		//AndWhere(sql.Neq(sql.NewRawObject("pre_final.value"), sql.NewIntVal(0))).
		Join(sql.NewJoin(
			"ANY LEFT",
			sql.NewWithRef(withLabels),
			sql.Eq(
				sql.NewRawObject("pre_final.fingerprint"),
				sql.NewRawObject(withLabels.GetAlias()+".new_fingerprint")))).
		GroupBy(sql.NewRawObject(withLabels.GetAlias() + ".labels"))
	return res, nil
}
