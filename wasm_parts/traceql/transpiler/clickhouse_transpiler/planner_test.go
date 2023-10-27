package clickhouse_transpiler

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
	sql "wasm_parts/sql_select"
	traceql_parser "wasm_parts/traceql/parser"
	"wasm_parts/traceql/shared"
)

func TestPlanner(t *testing.T) {
	script, err := traceql_parser.Parse(`{.randomContainer=~"admiring" && .randomFloat > 10}`)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := Plan(script)
	if err != nil {
		t.Fatal(err)
	}

	req, err := plan.Process(&shared.PlannerContext{
		IsCluster:            false,
		OrgID:                "0",
		From:                 time.Now().Add(time.Hour * -44),
		To:                   time.Now(),
		Limit:                3,
		TracesAttrsTable:     "tempo_traces_attrs_gin",
		TracesAttrsDistTable: "tempo_traces_attrs_gin_dist",
		TracesTable:          "tempo_traces",
		TracesDistTable:      "tempo_traces_dist",
	})
	if err != nil {
		t.Fatal(err)
	}
	res, err := req.String(&sql.Ctx{
		Params: map[string]sql.SQLObject{},
		Result: map[string]sql.SQLObject{},
	})
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(res)
}

func TestRandom(t *testing.T) {
	fmt.Sprintf("%f", 50+(rand.Float64()*100-50))
}
