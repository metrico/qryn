package traceql_ir

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func cmp(attr, op string, kind ValueKind, text string) *Comparison {
	return &Comparison{Attr: attr, Op: op, Value: Value{Kind: kind, Text: text}}
}

func strCmp(attr, op, text string) *Comparison { return cmp(attr, op, ValueString, text) }

func sel(f FilterExpr) *Selector { return &Selector{Filter: f} }

func a() *Selector { return sel(strCmp(".a", "=", `"a"`)) }
func b() *Selector { return sel(strCmp(".b", "=", `"b"`)) }
func c() *Selector { return sel(strCmp(".c", "=", `"c"`)) }
func d() *Selector { return sel(strCmp(".d", "=", `"d"`)) }

func op(o SpansetOperator, lhs, rhs SpansetExpr) *SpansetOp {
	return &SpansetOp{Op: o, LHS: lhs, RHS: rhs}
}

func pipeline(e SpansetExpr) Pipeline { return Pipeline{Elements: []Element{e}} }

func mustParse(t *testing.T, q string) *Query {
	t.Helper()
	got, err := Parse(q)
	require.NoError(t, err)
	return got
}

func TestLowerFilters(t *testing.T) {
	for _, tc := range []struct {
		query string
		want  FilterExpr
	}{
		{`{}`, nil},
		{`{.a = "b"}`, strCmp(".a", "=", `"b"`)},
		{"{.a = `b`}", strCmp(".a", "=", "`b`")},
		{`{span.x != 200}`, cmp("span.x", "!=", ValueNumber, "200")},
		{`{.a > -3}`, cmp(".a", ">", ValueNumber, "-3")},
		{`{.a < 1 . 5}`, cmp(".a", "<", ValueNumber, "1.5")},
		{`{duration > 1.5ms}`, cmp("duration", ">", ValueDuration, "1.5ms")},
		{`{.a = nil}`, cmp(".a", "=", ValueNil, "nil")},
		{`{.a = true}`, cmp(".a", "=", ValueBool, "true")},
		{`{status = error}`, cmp("status", "=", ValueIdent, "error")},
		{`{.a}`, cmp(".a", "", ValueNone, "")},
		{`{ false }`, cmp("false", "", ValueNone, "")},
		{`{.a = "b" && .c = "d"}`,
			&FilterOp{Op: "&&", LHS: strCmp(".a", "=", `"b"`), RHS: strCmp(".c", "=", `"d"`)}},
		{`{.a = "b" && .c = "d" || .e = "f"}`,
			&FilterOp{Op: "&&", LHS: strCmp(".a", "=", `"b"`),
				RHS: &FilterOp{Op: "||", LHS: strCmp(".c", "=", `"d"`), RHS: strCmp(".e", "=", `"f"`)}}},
		{`{(.a = 1 || .b = 2) && .c = 3}`,
			&FilterOp{Op: "&&",
				LHS: &FilterOp{Op: "||", LHS: cmp(".a", "=", ValueNumber, "1"), RHS: cmp(".b", "=", ValueNumber, "2")},
				RHS: cmp(".c", "=", ValueNumber, "3")}},
	} {
		t.Run(tc.query, func(t *testing.T) {
			require.Equal(t, &Query{Pipeline: pipeline(sel(tc.want))}, mustParse(t, tc.query))
		})
	}
}

func TestLowerAggregator(t *testing.T) {
	got := mustParse(t, `{.a = "a"} | avg(duration) >= 1.5ms`)
	require.Equal(t, &Query{Pipeline: pipeline(&Selector{
		Filter:     strCmp(".a", "=", `"a"`),
		Aggregator: &Aggregator{Fn: "avg", Attr: "duration", Cmp: ">=", Value: "1.5ms"},
	})}, got)
}

func TestLowerSpansetOperators(t *testing.T) {
	for _, tc := range []struct {
		query string
		want  SpansetExpr
	}{
		{`{.a = "a"} && {.b = "b"}`, op(OpAnd, a(), b())},
		{`{.a = "a"} || {.b = "b"}`, op(OpOr, a(), b())},
		{`{.a = "a"} >> {.b = "b"}`, op(OpDescendant, a(), b())},
		{`{.a = "a"} << {.b = "b"}`, op(OpAncestor, a(), b())},
		{`{.a = "a"} !>> {.b = "b"}`, op(OpNotDescendant, a(), b())},
		{`{.a = "a"} !<< {.b = "b"}`, op(OpNotAncestor, a(), b())},
		{`{.a = "a"} &>> {.b = "b"}`, op(OpUnionDescendant, a(), b())},
		{`{.a = "a"} &<< {.b = "b"}`, op(OpUnionAncestor, a(), b())},
		{`{.a = "a"} ~ {.b = "b"}`, op(OpSibling, a(), b())},
		{`{.a = "a"} > {.b = "b"}`, op(OpChild, a(), b())},
		{`{.a = "a"} < {.b = "b"}`, op(OpParent, a(), b())},
		{`{.a = "a"} !> {.b = "b"}`, op(OpNotChild, a(), b())},
		{`{.a = "a"} !< {.b = "b"}`, op(OpNotParent, a(), b())},
		{`{.a = "a"} !~ {.b = "b"}`, op(OpNotSibling, a(), b())},
		{`{.a = "a"} &> {.b = "b"}`, op(OpUnionChild, a(), b())},
		{`{.a = "a"} &< {.b = "b"}`, op(OpUnionParent, a(), b())},
		{`{.a = "a"} &~ {.b = "b"}`, op(OpUnionSibling, a(), b())},
		// Structural operators bind tighter than `&&` = `||`; all left-associative.
		{`{.a = "a"} && {.b = "b"} && {.c = "c"}`, op(OpAnd, op(OpAnd, a(), b()), c())},
		{`{.a = "a"} || {.b = "b"} || {.c = "c"}`, op(OpOr, op(OpOr, a(), b()), c())},
		{`{.a = "a"} && {.b = "b"} || {.c = "c"}`, op(OpOr, op(OpAnd, a(), b()), c())},
		{`{.a = "a"} || {.b = "b"} && {.c = "c"}`, op(OpAnd, op(OpOr, a(), b()), c())},
		{`{.a = "a"} ~ {.b = "b"} ~ {.c = "c"}`, op("~", op("~", a(), b()), c())},
		{`{.a = "a"} ~ {.b = "b"} && {.c = "c"} || {.d = "d"}`,
			op(OpOr, op(OpAnd, op("~", a(), b()), c()), d())},
		{`{.a = "a"} || {.b = "b"} &>> {.c = "c"} || {.d = "d"}`,
			op(OpOr, op(OpOr, a(), op(OpUnionDescendant, b(), c())), d())},
		{`{.a = "a"} && {.b = "b"} &>> {.c = "c"} !>> {.d = "d"}`,
			op(OpAnd, a(), op(OpNotDescendant, op(OpUnionDescendant, b(), c()), d()))},
		{`{.a = "a"} >> {.b = "b"} < {.c = "c"} << {.d = "d"}`,
			op(OpAncestor, op(OpParent, op(OpDescendant, a(), b()), c()), d())},
		{`{.a = "a"} << {.b = "b"} || {.c = "c"} >> {.d = "d"}`,
			op(OpOr, op(OpAncestor, a(), b()), op(OpDescendant, c(), d()))},
		{`{.a = "a"} > {.b = "b"} > {.c = "c"}`, op(OpChild, op(OpChild, a(), b()), c())},
		{`{.a = "a"} > ({.b = "b"} > {.c = "c"})`, op(OpChild, a(), &Group{Expr: op(OpChild, b(), c())})},
		{`{.a = "a"} !~ {.b = "b"} &< {.c = "c"} &~ {.d = "d"}`,
			op(OpUnionSibling, op(OpUnionParent, op(OpNotSibling, a(), b()), c()), d())},
		{`{.a = "a"} || {.b = "b"} > {.c = "c"} && {.d = "d"}`,
			op(OpAnd, op(OpOr, a(), op(OpChild, b(), c())), d())},
		{`{.a = "a"} < {.b = "b"} || {.c = "c"} !< {.d = "d"}`,
			op(OpOr, op(OpParent, a(), b()), op(OpNotParent, c(), d()))},
		{`{.a = "a"} &&`, a()},
		{`({.a = "a"})`, &Group{Expr: a()}},
		{`(({.a = "a"}))`, &Group{Expr: &Group{Expr: a()}}},
		{`({.a = "a"} || {.b = "b"}) && {.c = "c"}`,
			op(OpAnd, &Group{Expr: op(OpOr, a(), b())}, c())},
		{`{.c = "c"} && ({.a = "a"} || {.b = "b"})`,
			op(OpAnd, c(), &Group{Expr: op(OpOr, a(), b())})},
	} {
		t.Run(tc.query, func(t *testing.T) {
			require.Equal(t, &Query{Pipeline: pipeline(tc.want)}, mustParse(t, tc.query))
		})
	}
}

func TestLowerMetrics(t *testing.T) {
	p := 0.9
	from, to := int64(1), int64(2)
	for _, tc := range []struct {
		query string
		want  *Query
	}{
		{`{.a = "a"} | rate()`, &Query{Pipeline: pipeline(a()), Metrics: &MetricsFn{Fn: "rate"}}},
		{`{.a = "a"} | count_over_time() by (span.x, name)`, &Query{Pipeline: pipeline(a()),
			Metrics: &MetricsFn{Fn: "count_over_time", By: []string{"span.x", "name"}}}},
		{`{.a = "a"} | quantile_over_time(duration, 0.9)`, &Query{Pipeline: pipeline(a()),
			Metrics: &MetricsFn{Fn: "quantile_over_time", Attr: "duration", Percentile: &p}}},
		{`{.a = "a"} | compare({.b = "b"}, 10, 1, 2)`, &Query{Pipeline: pipeline(a()),
			Metrics: &MetricsFn{Fn: "compare", Compare: &CompareArgs{
				Filter: strCmp(".b", "=", `"b"`), Count: 10, BaselineFrom: &from, BaselineTo: &to}}}},
		{`{.a = "a"} | compare({})`, &Query{Pipeline: pipeline(a()),
			Metrics: &MetricsFn{Fn: "compare", Compare: &CompareArgs{}}}},
		{`({.a = "a"} &>> {.b = "b"}) | rate()`, &Query{
			Pipeline: pipeline(&Group{Expr: op(OpUnionDescendant, a(), b())}), Metrics: &MetricsFn{Fn: "rate"}}},
		{`({.a = "a"} | rate())`, &Query{
			Pipeline: pipeline(&Group{Expr: a()}), Metrics: &MetricsFn{Fn: "rate"}}},
		// A metrics function after a later chain operand is not the query's.
		{`{.a = "a"} && {.b = "b"} | rate()`, &Query{Pipeline: pipeline(op(OpAnd, a(), b()))}},
		{`{.a = "a"} | rate() with(sample=0.1, exemplars=false) | topk(5)`, &Query{
			Pipeline:    pipeline(a()),
			Metrics:     &MetricsFn{Fn: "rate"},
			Hints:       []Hint{{Key: "sample", Value: "0.1"}, {Key: "exemplars", Value: "false"}},
			SecondStage: &SecondStage{Fn: "topk", Count: 5},
		}},
		{`{.a = "a"} | rate() | select(span.a, duration) with(sample=0.5)`, &Query{
			Pipeline: pipeline(a()),
			Metrics:  &MetricsFn{Fn: "rate"},
			SecondStage: &SecondStage{Fn: "select", Labels: []string{"span.a", "duration"},
				Hints: []Hint{{Key: "sample", Value: "0.5"}}},
		}},
	} {
		t.Run(tc.query, func(t *testing.T) {
			require.Equal(t, tc.want, mustParse(t, tc.query))
		})
	}
}

func TestValueUnquote(t *testing.T) {
	for raw, want := range map[string]string{`"a\"b"`: `a"b`, "`a\\b`": `a\b`} {
		got, err := Value{Kind: ValueString, Text: raw}.Unquote()
		require.NoError(t, err)
		require.Equal(t, want, got)
	}
}

func TestParseError(t *testing.T) {
	_, err := Parse(`{`)
	require.Error(t, err)
}

func TestHasStructural(t *testing.T) {
	for query, want := range map[string]bool{
		`{.a = "a"}`:                                false,
		`{.a = "a"} && {.b = "b"} || {.c = "c"}`:    false,
		`{.a = "a"} > {.b = "b"}`:                   true,
		`{.a = "a"} !~ {.b = "b"}`:                  true,
		`{.a = "a"} >> {.b = "b"}`:                  true,
		`{.a = "a"} && ({.b = "b"} &<< {.c = "c"})`: true,
		`({.a = "a"} !<< {.b = "b"}) | rate()`:      true,
	} {
		require.Equal(t, want, mustParse(t, query).HasStructural(), query)
	}
}
