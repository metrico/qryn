package clickhouse_transpiler

import (
	"fmt"
	"regexp"
	"strings"
	"testing"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	traceql_ir "github.com/metrico/qryn/v5/reader/traceql/traceql_ir"
	sql "github.com/metrico/qryn/v5/reader/utils/sql_select"
)

func TestSearchPlansOneHopOperators(t *testing.T) {
	for _, c := range []struct{ query, want string }{
		{`{.a = 1} > {.b = 2}`, `>({.a},{.b})`},
		{`{.a = 1} < {.b = 2}`, `<({.a},{.b})`},
		{`{.a = 1} ~ {.b = 2}`, `~({.a},{.b})`},
		{`{.a = 1} !> {.b = 2}`, `!>({.a},{.b})`},
		{`{.a = 1} !< {.b = 2}`, `!<({.a},{.b})`},
		{`{.a = 1} !~ {.b = 2}`, `!~({.a},{.b})`},
		{`{.a = 1} &> {.b = 2}`, `&>({.a},{.b})`},
		{`{.a = 1} &< {.b = 2}`, `&<({.a},{.b})`},
		{`{.a = 1} &~ {.b = 2}`, `&~({.a},{.b})`},
		{`{.a = 1} > {.b = 2} > {.c = 3}`, `>(>({.a},{.b}),{.c})`},
		{`{.a = 1} > ({.b = 2} > {.c = 3})`, `>({.a},>({.b},{.c}))`},
		{`{.a = 1} || {.b = 2} > {.c = 3} && {.d = 4}`, `and(or({.a},>({.b},{.c})),{.d})`},
		{`{} > {.b = 2}`, `>({},{.b})`},
		{`{.a = 1} &>> {.b = 2}`, `and({.a},{.b})`},
		{`{.a = 1} !>> {.b = 2}`, `and({.a},{.b})`},
	} {
		t.Run(c.query, func(t *testing.T) {
			_, expr := searchStages(t, planSearch(t, c.query))
			if got := describePlan(t, expr); got != c.want {
				t.Errorf("plan = %s, want %s", got, c.want)
			}
		})
	}
}

// structuralWiths returns the CTEs of a rendered-ready search plan by alias.
func structuralWiths(t *testing.T, query string, ctx *shared.PlannerContext) map[string]*sql.With {
	t.Helper()
	req, err := planSearch(t, query).Process(ctx)
	if err != nil {
		t.Fatal(err)
	}
	res := map[string]*sql.With{}
	for _, w := range req.GetWith() {
		res[w.GetAlias()] = w
	}
	return res
}

func renderWith(t *testing.T, w *sql.With) string {
	t.Helper()
	res, err := w.String(sql.DefaultCtx(), sql.STRING_OPT_SKIP_WITH)
	if err != nil {
		t.Fatal(err)
	}
	return res
}

func withSuffix(withs map[string]*sql.With, suffix string) []*sql.With {
	var res []*sql.With
	for alias, w := range withs {
		if strings.HasSuffix(alias, suffix) {
			res = append(res, w)
		}
	}
	return res
}

// One-hop operators relate spans in one pass over their parent keys. The only
// recursion is root reachability, seeded from traces with a missing parent.
func TestOneHopRecursesOnlyForIncompleteTraces(t *testing.T) {
	for _, query := range structuralQueries {
		t.Run(query, func(t *testing.T) {
			withs := structuralWiths(t, query, testPlannerContext(3))
			var recursive []*sql.With
			for _, w := range withs {
				if w.IsRecursive() {
					recursive = append(recursive, w)
				}
			}
			if len(recursive) != 1 || !strings.HasSuffix(recursive[0].GetAlias(), "_jump") {
				t.Fatalf("want one recursive reachability CTE, got %d", len(recursive))
			}
			anchor, err := recursive[0].GetQuery().String(sql.DefaultCtx(), sql.STRING_OPT_SKIP_WITH)
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(anchor, "has_root = 1 AND has_dangling = 1") {
				t.Errorf("reachability is not limited to traces with a missing parent:\n%s", anchor)
			}
			step, err := recursive[0].GetStep().String(sql.DefaultCtx(), sql.STRING_OPT_SKIP_WITH)
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(step, fmt.Sprintf("((x.k) < (%d))", reachRounds)) {
				t.Errorf("reachability rounds are unbounded:\n%s", step)
			}
			for _, w := range withSuffix(withs, "_counted") {
				if res := renderWith(t, w); !strings.Contains(res, "OVER (PARTITION BY trace_id, reachable, key)") {
					t.Errorf("relation is not one pass over parent keys:\n%s", res)
				}
			}
		})
	}
}

func TestOneHopEdgesComeFromTheTracesTableWithoutTimeBound(t *testing.T) {
	for _, cluster := range []bool{false, true} {
		ctx := testPlannerContext(3)
		ctx.IsCluster = cluster
		table := "FROM tempo_traces as traces"
		in := " IN ("
		if cluster {
			table = "FROM tempo_traces_dist as traces"
			in = " GLOBAL IN ("
		}
		edges := withSuffix(structuralWiths(t, `{.a = 1} > {.b = 2}`, ctx), "_edges")
		if len(edges) != 1 {
			t.Fatalf("cluster=%v: %d edge CTEs, want 1", cluster, len(edges))
		}
		res := renderWith(t, edges[0])
		if !strings.Contains(res, table) || strings.Count(res, "traces.trace_id"+in) != 2 {
			t.Errorf("cluster=%v: edges are not read from the traces table by trace id:\n%s", cluster, res)
		}
		if strings.Contains(res, "timestamp_ns") || strings.Contains(res, "date") {
			t.Errorf("cluster=%v: edges are bounded in time:\n%s", cluster, res)
		}
	}
}

func TestEnableAnalyzerOnlyOnStructuralQueries(t *testing.T) {
	setting := regexp.MustCompile(`enable_analyzer`)
	for _, query := range searchQueries {
		t.Run(query, func(t *testing.T) {
			res := renderPlan(t, planSearch(t, query), testPlannerContext(3))
			want := 0
			for _, q := range structuralQueries {
				if q == query {
					want = 1
				}
			}
			if n := len(setting.FindAllString(res, -1)); n != want {
				t.Errorf("enable_analyzer appears %d times, want %d:\n%s", n, want, res)
			}
			if want == 1 && !strings.HasSuffix(res, " SETTINGS enable_analyzer=1") {
				t.Errorf("enable_analyzer is not a query setting:\n%s", res)
			}
		})
	}
}

func TestStructuralQueriesNeedClickHouse249(t *testing.T) {
	ctx := testPlannerContext(3)
	ctx.VersionInfo = map[string]int64{}
	for _, query := range structuralQueries {
		if _, err := planSearch(t, query).Process(ctx); err != errStructuralVersion {
			t.Errorf("%s: error = %v, want %v", query, err, errStructuralVersion)
		}
	}
	for _, query := range plainSearchQueries {
		if _, err := planSearch(t, query).Process(ctx); err != nil {
			t.Errorf("%s: %v", query, err)
		}
	}
}

// `{}` under a spanset operator is every span in range, not the attribute-less
// planner's trace pre-pick.
func TestEmptyOperandReadsEverySpanInRange(t *testing.T) {
	for _, query := range []string{`{} > {.a = 1}`, `{.a = 1} && {}`} {
		t.Run(query, func(t *testing.T) {
			res := renderPlan(t, planSearch(t, query), testPlannerContext(3))
			if strings.Contains(res, "attrless_trace_ids") {
				t.Errorf("operand {} uses the trace pre-pick:\n%s", res)
			}
		})
	}
	q, err := traceql_ir.Parse(`{} ~ {.a = 1}`)
	if err != nil {
		t.Fatal(err)
	}
	eval, err := PlanEval(q)
	if err != nil {
		t.Fatal(err)
	}
	operands := eval.(*EvalFinalizerPlanner).Main.(*ComplexEvalOrPlanner).Operands
	if _, ok := operands[0].(*AllSpansEvaluatorPlanner); !ok {
		t.Errorf("operand {} is estimated by %T", operands[0])
	}
}
