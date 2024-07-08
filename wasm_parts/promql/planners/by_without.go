package planners

import (
	"fmt"
	"strings"
	"wasm_parts/promql/shared"
	sql "wasm_parts/sql_select"
)

type ByWithoutPlanner struct {
	Main                shared.RequestPlanner
	FingerprintWithName string
	FingerprintsOutName string
	ByWithout           string
	Labels              []string
}

func (b *ByWithoutPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	main, err := b.Main.Process(ctx)
	if err != nil {
		return nil, err
	}
	var fp *sql.With
	withs := main.GetWith()
	for _, w := range withs {
		if w.GetAlias() == b.FingerprintWithName {
			fp = w
			break
		}
	}
	if fp == nil {
		return nil, fmt.Errorf("fingerprints subrequest not found")
	}
	labelsCol := sql.NewCustomCol(func(ctx *sql.Ctx, options ...int) (string, error) {
		cond := "IN"
		if b.ByWithout == "without" {
			cond = "NOT IN"
		}
		values := make([]string, len(b.Labels))
		var err error
		for i, l := range b.Labels {
			values[i], err = sql.NewStringVal(l).String(ctx, options...)
			if err != nil {
				return "", err
			}
		}
		return fmt.Sprintf("mapFilter((k,v) -> k %s (%s), labels)", cond, strings.Join(values, ",")), nil
	})
	newFpCol := "cityHash64(arraySort(arrayZip(mapKeys(labels), mapValues(labels))))"
	newFp := sql.NewSelect().
		Select(
			sql.NewSimpleCol(fp.GetAlias()+".new_fingerprint", "fingerprint"),
			sql.NewCol(labelsCol, "labels"),
			sql.NewSimpleCol(newFpCol, "new_fingerprint"),
		).
		From(sql.NewWithRef(fp))
	withNewFp := sql.NewWith(newFp, b.FingerprintsOutName)
	return main.AddWith(withNewFp), nil
}
