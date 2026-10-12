package traceql_parser

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func term(label, op string, val Value) *AttrSelectorExp {
	return &AttrSelectorExp{Head: &AttrSelector{Label: label, Op: op, Val: val}}
}

func join(head *AttrSelectorExp, andOr string, tail *AttrSelectorExp) *AttrSelectorExp {
	head.AndOr = andOr
	head.Tail = tail
	return head
}

func str(s string) Value { return Value{StrVal: &QuotedString{Str: s}} }

func sel(exp *AttrSelectorExp) Operand { return Operand{Selector: Selector{AttrSelector: exp}} }

func expr(head Operand, ops ...SpansetOperation) *SpansetExpr {
	return &SpansetExpr{Head: head, Ops: ops}
}

func then(op string, operand Operand) SpansetOperation {
	return SpansetOperation{Op: op, Operand: operand}
}

func paren(head *SpansetExpr, stages ...PipelineElement) Operand {
	return Operand{Paren: &ParenPipeline{Head: *head, Stages: stages}}
}

func spanset(e *SpansetExpr) PipelineElement { return PipelineElement{Spanset: e} }

// script is a query of one spanset expression followed by stages.
func script(e *SpansetExpr, stages ...PipelineElement) *TraceQLScript {
	return &TraceQLScript{Pipeline: Pipeline{Head: spanset(e), Stages: stages}}
}

func scalar(atom ScalarAtom) ScalarExpr {
	return ScalarExpr{Head: ScalarTerm{Head: ScalarFactor{Base: atom}}}
}

func agg(fn, attr string) ScalarExpr {
	return scalar(ScalarAtom{Aggregate: &Aggregate{Fn: fn, Attr: attr}})
}

func lit(num, unit string) ScalarExpr {
	return scalar(ScalarAtom{Literal: &ScalarLiteral{Num: num, Unit: unit}})
}

func filter(lhs ScalarExpr, op string, rhs ScalarExpr) PipelineElement {
	return PipelineElement{ScalarFilter: &ScalarFilter{LHS: lhs, Op: op, RHS: rhs}}
}

func f64(f float64) *float64 { return &f }

func i64(i int64) *int64 { return &i }

func TestParseSelectors(t *testing.T) {
	for _, c := range []struct {
		query string
		want  *AttrSelectorExp
	}{
		{`{}`, nil},
		{`{.a = "b"}`, term(".a", "=", str(`"b"`))},
		{"{.a = `b`}", term(".a", "=", str("`b`"))},
		{`{span.http.status_code != 200}`, term("span.http.status_code", "!=", Value{FVal: "200"})},
		{`{resource.service.name =~ "api-.*"}`, term("resource.service.name", "=~", str(`"api-.*"`))},
		{`{.a !~ "x"}`, term(".a", "!~", str(`"x"`))},
		{`{.a > -3}`, term(".a", ">", Value{FVal: "-3"})},
		{`{.a >= 1.5}`, term(".a", ">=", Value{FVal: "1.5"})},
		{`{.a < 1 . 5}`, term(".a", "<", Value{FVal: "1.5"})},
		{`{.a <= 0}`, term(".a", "<=", Value{FVal: "0"})},
		{`{duration > 1.5ms}`, term("duration", ">", Value{TimeVal: "1.5ms"})},
		{`{span:duration < 2s}`, term("span:duration", "<", Value{TimeVal: "2s"})},
		{`{.a = nil}`, term(".a", "=", Value{NilVal: "nil"})},
		{`{.a != nil}`, term(".a", "!=", Value{NilVal: "nil"})},
		{`{.a = true}`, term(".a", "=", Value{BoolVal: "true"})},
		{`{status = error}`, term("status", "=", Value{UnquotVal: "error"})},
		{`{kind = server}`, term("kind", "=", Value{UnquotVal: "server"})},
		{`{.a-b.c = "x"}`, term(".a-b.c", "=", str(`"x"`))},
		{`{.a}`, term(".a", "", Value{})},
		{`{.a = }`, term(".a", "=", Value{})},
		{`{.a = .b}`, term(".a", "=", Value{UnquotVal: ".b"})},
		{`{ true }`, term("true", "", Value{})},
		{`{.a = "b" && .c = "d"}`,
			join(term(".a", "=", str(`"b"`)), "&&", term(".c", "=", str(`"d"`)))},
		{`{.a = "b" || .c = "d" && .e = "f"}`,
			join(term(".a", "=", str(`"b"`)), "||",
				join(term(".c", "=", str(`"d"`)), "&&", term(".e", "=", str(`"f"`))))},
		{`{(.a = 1 || .b = 2) && .c = 3}`,
			&AttrSelectorExp{
				ComplexHead: join(term(".a", "=", Value{FVal: "1"}), "||", term(".b", "=", Value{FVal: "2"})),
				AndOr:       "&&",
				Tail:        term(".c", "=", Value{FVal: "3"}),
			}},
	} {
		t.Run(c.query, func(t *testing.T) {
			got, err := Parse(c.query)
			require.NoError(t, err)
			require.Equal(t, script(expr(sel(c.want))), got)
		})
	}
}

func TestParseSpansetOperators(t *testing.T) {
	a := func() Operand { return sel(term(".a", "=", str(`"b"`))) }
	c := func() Operand { return sel(term(".c", "=", str(`"d"`))) }
	for _, op := range []string{
		"&&", "||",
		">", "<", "~", "!>", "!<", "!~", "&>", "&<", "&~",
		">>", "<<", "!>>", "!<<", "&>>", "&<<",
	} {
		t.Run(op, func(t *testing.T) {
			want := script(expr(a(), then(op, c())))
			got, err := Parse(`{.a = "b"} ` + op + ` {.c = "d"}`)
			require.NoError(t, err)
			require.Equal(t, want, got)
			got, err = Parse(`{.a = "b"}` + op + `{.c = "d"}`)
			require.NoError(t, err)
			require.Equal(t, want, got)
		})
	}

	t.Run("chain in written order", func(t *testing.T) {
		got, err := Parse(`{.a = "b"} && {.c = "d"} || {}`)
		require.NoError(t, err)
		require.Equal(t, script(expr(a(), then("&&", c()), then("||", sel(nil)))), got)
	})

	t.Run("parenthesised expression", func(t *testing.T) {
		got, err := Parse(`({.a = "b"} &>> {.c = "d"}) || {}`)
		require.NoError(t, err)
		require.Equal(t, script(expr(paren(expr(a(), then("&>>", c()))), then("||", sel(nil)))), got)
	})

	// Inside {…} >, < and !~ compare; between spansets they are structural.
	for _, c := range []struct {
		query string
		want  *TraceQLScript
	}{
		{`{ .x > 3 } > { .y !~ "a.*" }`, script(expr(
			sel(term(".x", ">", Value{FVal: "3"})), then(">", sel(term(".y", "!~", str(`"a.*"`))))))},
		{`{.x<3}<{.y<4}`, script(expr(
			sel(term(".x", "<", Value{FVal: "3"})), then("<", sel(term(".y", "<", Value{FVal: "4"})))))},
		{`{ .x !~ "a" } !~ { .y =~ "b" }`, script(expr(
			sel(term(".x", "!~", str(`"a"`))), then("!~", sel(term(".y", "=~", str(`"b"`))))))},
		{`{ .x >= 1 } &> { .y <= 2 }`, script(expr(
			sel(term(".x", ">=", Value{FVal: "1"})), then("&>", sel(term(".y", "<=", Value{FVal: "2"})))))},
		{`{ .x = "<<&" } > {}`, script(expr(sel(term(".x", "=", str(`"<<&"`))), then(">", sel(nil))))},
	} {
		t.Run(c.query, func(t *testing.T) {
			got, err := Parse(c.query)
			require.NoError(t, err)
			require.Equal(t, c.want, got)
		})
	}

	for _, q := range []string{
		// Split spellings.
		`{.a = "b"} ! > {.c = "d"}`,
		`{.a = "b"} ! < {.c = "d"}`,
		`{.a = "b"} ! ~ {.c = "d"}`,
		`{.a = "b"} & > {.c = "d"}`,
		`{.a = "b"} & < {.c = "d"}`,
		`{.a = "b"} & ~ {.c = "d"}`,
		`{.a = "b"} > > {.c = "d"}`,
		`{.a = "b"} < < {.c = "d"}`,
		`{.a = "b"} & >> {.c = "d"}`,
		`{.a = "b"} ! >> {.c = "d"}`,
		`{.a = "b"} & << {.c = "d"}`,
		`{.a = "b"} ! << {.c = "d"}`,
		`{.a = "b"} > >> {.c = "d"}`,
		`{.a = "b"} << < {.c = "d"}`,
		`{.a >> 1}`,
		// Structural operators inside a selector.
		`{.a > {.c = "d"}}`,
		`{.a &> 1}`,
		// A dangling operator.
		`{.a = "b"} &&`,
		`{.a = "b"} >> | count() > 1`,
	} {
		t.Run("rejects "+q, func(t *testing.T) {
			_, err := Parse(q)
			require.Error(t, err)
		})
	}

	for op, spelling := range map[string]string{"<<&": "&<<", "<<~": "!<<"} {
		t.Run("rejects misspelt "+op, func(t *testing.T) {
			_, err := Parse(`{.a = "b"} ` + op + ` {.c = "d"}`)
			require.EqualError(t, err, op+" is not a TraceQL operator, use "+spelling)
		})
	}
}

func TestParseScalarFilters(t *testing.T) {
	a := term(".a", "=", str(`"b"`))
	for _, c := range []struct {
		query string
		want  *TraceQLScript
	}{
		{`{.a = "b"} | count() > 2`, script(expr(sel(a)), filter(agg("count", ""), ">", lit("2", "")))},
		{`{} | count() = 3.5`, script(expr(sel(nil)), filter(agg("count", ""), "=", lit("3.5", "")))},
		{`{} | avg(duration) >= 1.5ms`, script(expr(sel(nil)), filter(agg("avg", "duration"), ">=", lit("1.5", "ms")))},
		{`{} | max(span.x) < -3`, script(expr(sel(nil)), filter(agg("max", "span.x"), "<", lit("-3", "")))},
		{`{} | min(.x) != 0`, script(expr(sel(nil)), filter(agg("min", ".x"), "!=", lit("0", "")))},
		{`{} | sum(resource.x) <= 10s`, script(expr(sel(nil)), filter(agg("sum", "resource.x"), "<=", lit("10", "s")))},
		{`{} | avg(span:duration) > 1 . 5`, script(expr(sel(nil)), filter(agg("avg", "span:duration"), ">", lit("1.5", "")))},
		// A scalar filter applies to the whole expression before it.
		{`{.a = "b"} && {} | count() > 1`, script(expr(sel(a), then("&&", sel(nil))),
			filter(agg("count", ""), ">", lit("1", "")))},
		{`{.a = "b"} >> {} | count() > 1 | avg(duration) > 2ms`, script(expr(sel(a), then(">>", sel(nil))),
			filter(agg("count", ""), ">", lit("1", "")), filter(agg("avg", "duration"), ">", lit("2", "ms")))},
		// Forms that parse and fail validation.
		{`{} | 2 < count()`, script(expr(sel(nil)), filter(lit("2", ""), "<", agg("count", "")))},
		{`{} | count() > count()`, script(expr(sel(nil)), filter(agg("count", ""), ">", agg("count", "")))},
		{`{} | sum(.a) + 1 > 0`, script(expr(sel(nil)), filter(ScalarExpr{
			Head: ScalarTerm{Head: ScalarFactor{Base: ScalarAtom{Aggregate: &Aggregate{Fn: "sum", Attr: ".a"}}}},
			Ops:  []ScalarSumOp{{Op: "+", Term: ScalarTerm{Head: ScalarFactor{Base: ScalarAtom{Literal: &ScalarLiteral{Num: "1"}}}}}},
		}, ">", lit("0", "")))},
	} {
		t.Run(c.query, func(t *testing.T) {
			got, err := Parse(c.query)
			require.NoError(t, err)
			require.Equal(t, c.want, got)
		})
	}

	t.Run("a bare scalar filter", func(t *testing.T) {
		got, err := Parse(`count() > 2`)
		require.NoError(t, err)
		require.Equal(t, &TraceQLScript{Pipeline: Pipeline{Head: filter(agg("count", ""), ">", lit("2", ""))}}, got)
	})

	t.Run("arithmetic precedence", func(t *testing.T) {
		got, err := Parse(`{} | count() - 1 * 2 ^ 3 ^ 4 > (sum(.a) % 2)`)
		require.NoError(t, err)
		require.Equal(t, `{} | count() - 1 * 2 ^ 3 ^ 4 > (sum(.a) % 2)`, got.String())
		f := got.Pipeline.Stages[0].ScalarFilter
		require.Len(t, f.LHS.Ops, 1)
		pow := f.LHS.Ops[0].Term.Ops[0].Factor
		require.Equal(t, "2", pow.Base.Literal.Num)
		require.Equal(t, "3", pow.Exp.Base.Literal.Num)
		require.Equal(t, "4", pow.Exp.Exp.Base.Literal.Num)
		require.NotNil(t, f.RHS.Head.Head.Base.Paren)
	})

	for _, q := range []string{
		`{} | count() > "2"`,
		`{} | count() > 2 && avg(.a) > 1`,
		`{} | count() > 2 || avg(.a) > 1`,
		`{} | (count() > 2)`,
		`(count() > 2)`,
		`{} | (count() > 2) | avg(.a) > 1`,
		`count() > 3 && { true }`,
		`{} | median(duration) > 2`,
		`{} | count(.a) > 2`,
		`{} | avg() > 2`,
		`{} | count() > 2 | count() >`,
	} {
		t.Run("rejects "+q, func(t *testing.T) {
			_, err := Parse(q)
			require.Error(t, err)
		})
	}
}

func TestParsePipelines(t *testing.T) {
	a := func() Operand { return sel(term(".a", "=", str(`"b"`))) }
	c := func() Operand { return sel(term(".c", "=", str(`"d"`))) }
	count := filter(agg("count", ""), ">", lit("2", ""))
	for _, tc := range []struct {
		query string
		want  *TraceQLScript
	}{
		// A spanset expression at any position.
		{`{.a = "b"} | {.c = "d"}`, script(expr(a()), spanset(expr(c())))},
		{`{.a = "b"} | count() > 2 | {.c = "d"} > {}`, script(expr(a()), count,
			spanset(expr(c(), then(">", sel(nil)))))},
		// A parenthesised pipeline is an operand.
		{`({.a = "b"} | count() > 2) >> {.c = "d"}`, script(expr(paren(expr(a()), count), then(">>", c())))},
		{`{.c = "d"} && ({.a = "b"} | count() > 2)`, script(expr(c(), then("&&", paren(expr(a()), count))))},
		{`({.a = "b"} | count() > 2) | count() > 2`, script(expr(paren(expr(a()), count)), count)},
		{`(({.a = "b"} | count() > 2))`, script(expr(paren(expr(paren(expr(a()), count)))))},
		{`{.a = "b"} | ({.c = "d"} | count() > 2)`, script(expr(a()), spanset(expr(paren(expr(c()), count))))},
	} {
		t.Run(tc.query, func(t *testing.T) {
			got, err := Parse(tc.query)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestParseMetricsFunctions(t *testing.T) {
	a := func() *SpansetExpr { return expr(sel(term(".a", "=", str(`"b"`)))) }
	empty := func() *SpansetExpr { return expr(sel(nil)) }
	metrics := func(e *SpansetExpr, m *MetricsPipelineStage, stages ...PipelineElement) *TraceQLScript {
		res := script(e, stages...)
		res.MetricsFn = m
		return res
	}
	for _, c := range []struct {
		query string
		want  *TraceQLScript
	}{
		{`{.a = "b"} | rate()`, metrics(a(), &MetricsPipelineStage{Fn: "rate"})},
		{`{} | count_over_time() by (span.x, name)`, metrics(empty(),
			&MetricsPipelineStage{Fn: "count_over_time", By: &ByClause{Labels: []string{"span.x", "name"}}})},
		{`{} | quantile_over_time(duration, 0.9)`, metrics(empty(),
			&MetricsPipelineStage{Fn: "quantile_over_time", Attr: "duration", Percentile: f64(0.9)})},
		{`{} | histogram_over_time(duration)`, metrics(empty(),
			&MetricsPipelineStage{Fn: "histogram_over_time", Attr: "duration"})},
		{`{} | compare({.a = "b"}, 10, 1, 2)`, metrics(empty(), &MetricsPipelineStage{Fn: "compare", CompSel: &CompareSelector{
			AttrSelector: term(".a", "=", str(`"b"`)), Count: 10, BaselineFrom: i64(1), BaselineTo: i64(2),
		}})},
		{`{} | compare({})`, metrics(empty(), &MetricsPipelineStage{Fn: "compare", CompSel: &CompareSelector{}})},
		{`{.a = "b"} | count() > 2 | rate()`, metrics(a(), &MetricsPipelineStage{Fn: "rate"},
			filter(agg("count", ""), ">", lit("2", "")))},
		{`count() > 2 | rate()`, &TraceQLScript{
			Pipeline:  Pipeline{Head: filter(agg("count", ""), ">", lit("2", ""))},
			MetricsFn: &MetricsPipelineStage{Fn: "rate"},
		}},
		{`({} &>> {}) | rate()`, metrics(expr(paren(expr(sel(nil), then("&>>", sel(nil))))),
			&MetricsPipelineStage{Fn: "rate"})},
		{`{} | by(.a)`, metrics(empty(), &MetricsPipelineStage{Fn: "by", Attr: ".a"})},
		{`{} | coalesce()`, metrics(empty(), &MetricsPipelineStage{Fn: "coalesce"})},
		{`{} | select(.a)`, metrics(empty(), &MetricsPipelineStage{Fn: "select", Attr: ".a"})},
		{`{} && {} | rate()`, metrics(expr(sel(nil), then("&&", sel(nil))), &MetricsPipelineStage{Fn: "rate"})},
	} {
		t.Run(c.query, func(t *testing.T) {
			got, err := Parse(c.query)
			require.NoError(t, err)
			require.Equal(t, c.want, got)
		})
	}

	for _, q := range []string{`({} | rate())`, `{} > ({} | rate())`} {
		t.Run("rejects "+q, func(t *testing.T) {
			_, err := Parse(q)
			require.Error(t, err)
		})
	}
}

func TestParseWithHintsAndSecondStage(t *testing.T) {
	rate := func(rest *TraceQLScript) *TraceQLScript {
		rest.Pipeline = Pipeline{Head: spanset(expr(sel(nil)))}
		rest.MetricsFn = &MetricsPipelineStage{Fn: "rate"}
		return rest
	}
	for _, c := range []struct {
		query string
		want  *TraceQLScript
	}{
		{`{} | rate() with(sample=0.1, exemplars=false)`, rate(&TraceQLScript{
			WithHints: &WithClause{Hints: []WithHint{{Key: "sample", Value: "0.1"}, {Key: "exemplars", Value: "false"}}},
		})},
		{`{} | rate() with(sample=true, exemplars=5, most_recent=x)`, rate(&TraceQLScript{
			WithHints: &WithClause{Hints: []WithHint{
				{Key: "sample", Value: "true"}, {Key: "exemplars", Value: "5"}, {Key: "most_recent", Value: "x"},
			}},
		})},
		{`{} | rate() | topk(5)`, rate(&TraceQLScript{
			SecondStage: &SecondPipelineStage{Fn: "topk", Count: 5},
		})},
		{`{} | rate() | bottomk(3) with(sample=0.5)`, rate(&TraceQLScript{
			SecondStage: &SecondPipelineStage{Fn: "bottomk", Count: 3,
				WithHints: &WithClause{Hints: []WithHint{{Key: "sample", Value: "0.5"}}}},
		})},
		{`{} | rate() | select(span.a, duration)`, rate(&TraceQLScript{
			SecondStage: &SecondPipelineStage{Fn: "select", Labels: []string{"span.a", "duration"}},
		})},
		{`{} | rate() with(sample=0.1) | topk(1)`, rate(&TraceQLScript{
			WithHints:   &WithClause{Hints: []WithHint{{Key: "sample", Value: "0.1"}}},
			SecondStage: &SecondPipelineStage{Fn: "topk", Count: 1},
		})},
	} {
		t.Run(c.query, func(t *testing.T) {
			got, err := Parse(c.query)
			require.NoError(t, err)
			require.Equal(t, c.want, got)
		})
	}
}

func TestParseRejects(t *testing.T) {
	for _, q := range []string{
		``,
		`{`,
		`{.a = "b"`,
		`{.a + .b > 3}`,
		`{!(.a = 1)}`,
		`{.a = "b"} {.c = "d"}`,
		`({.a = "b"}) ({.c = "d"})`,
	} {
		t.Run(q, func(t *testing.T) {
			_, err := Parse(q)
			require.Error(t, err)
		})
	}
}

func TestParseConcurrent(t *testing.T) {
	queries := []string{`{.a = "b"} &>> {.c = "d"}`, `{} | rate() by (name)`, `{duration > 1s} | count() > 2`}
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(q string) {
			defer wg.Done()
			_, err := Parse(q)
			require.NoError(t, err)
		}(queries[i%len(queries)])
	}
	wg.Wait()
}
