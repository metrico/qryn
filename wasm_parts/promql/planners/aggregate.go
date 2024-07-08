package planners

import (
	"fmt"
	"wasm_parts/promql/shared"
	sql "wasm_parts/sql_select"
)

type SumPlanner struct {
	Main        shared.RequestPlanner
	LabelsAlias string
}

func (s *SumPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	main, err := s.Main.Process(ctx)
	if err != nil {
		return nil, err
	}

	var withLabels *sql.With
	for _, w := range main.GetWith() {
		if w.GetAlias() == s.LabelsAlias {
			withLabels = w
			break
		}
	}
	if withLabels == nil {
		return nil, fmt.Errorf("labels subrequest not found")
	}
	withMain := sql.NewWith(main, "pre_sum")

	res := sql.NewSelect().With(withMain).
		Select(
			sql.NewSimpleCol(withLabels.GetAlias()+".new_fingerprint", "fingerprint"),
			sql.NewSimpleCol("pre_sum.timestamp_ms", "timestamp_ms"),
			sql.NewSimpleCol("sum(pre_sum.value)", "value")).
		From(sql.NewWithRef(withMain)).
		Join(sql.NewJoin(
			"ANY LEFT",
			sql.NewWithRef(withLabels),
			sql.Eq(
				sql.NewRawObject("pre_sum.fingerprint"),
				sql.NewRawObject(withLabels.GetAlias()+".fingerprint")))).
		GroupBy(
			sql.NewRawObject(withLabels.GetAlias()+".new_fingerprint"),
			sql.NewRawObject("pre_sum.timestamp_ms"))
	return res, nil
}
