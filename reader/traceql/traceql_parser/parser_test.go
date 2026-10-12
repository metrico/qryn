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

func sel(exp *AttrSelectorExp) Selector { return Selector{AttrSelector: exp} }

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
			require.Equal(t, &TraceQLScript{Head: sel(c.want)}, got)
		})
	}
}

func TestParseSpansetOperators(t *testing.T) {
	a := func() Selector { return sel(term(".a", "=", str(`"b"`))) }
	c := func() Selector { return sel(term(".c", "=", str(`"d"`))) }
	for _, op := range []string{
		"&&", "||", "&>>", "!>>",
		">", "<", "~", "!>", "!<", "!~", "&>", "&<", "&~",
	} {
		t.Run(op, func(t *testing.T) {
			want := &TraceQLScript{Head: a(), Op: op, Tail: &TraceQLScript{Head: c()}}
			got, err := Parse(`{.a = "b"} ` + op + ` {.c = "d"}`)
			require.NoError(t, err)
			require.Equal(t, want, got)
			got, err = Parse(`{.a = "b"}` + op + `{.c = "d"}`)
			require.NoError(t, err)
			require.Equal(t, want, got)
		})
	}

	t.Run("right-nested chain", func(t *testing.T) {
		got, err := Parse(`{.a = "b"} && {.c = "d"} || {}`)
		require.NoError(t, err)
		require.Equal(t, &TraceQLScript{
			Head: a(), Op: "&&",
			Tail: &TraceQLScript{Head: c(), Op: "||", Tail: &TraceQLScript{}},
		}, got)
	})

	t.Run("trailing operator", func(t *testing.T) {
		got, err := Parse(`{.a = "b"} &&`)
		require.NoError(t, err)
		require.Equal(t, &TraceQLScript{Head: a(), Op: "&&"}, got)
	})

	t.Run("parenthesised script", func(t *testing.T) {
		got, err := Parse(`({.a = "b"} &>> {.c = "d"}) || {}`)
		require.NoError(t, err)
		require.Equal(t, &TraceQLScript{
			ParenExpr: &TraceQLScript{Head: a(), Op: "&>>", Tail: &TraceQLScript{Head: c()}},
			Op:        "||",
			Tail:      &TraceQLScript{},
		}, got)
	})

	// Inside {…} >, < and !~ compare; between spansets they are structural.
	for _, c := range []struct {
		query string
		want  *TraceQLScript
	}{
		{`{ .x > 3 } > { .y !~ "a.*" }`, &TraceQLScript{
			Head: sel(term(".x", ">", Value{FVal: "3"})), Op: ">",
			Tail: &TraceQLScript{Head: sel(term(".y", "!~", str(`"a.*"`)))},
		}},
		{`{.x<3}<{.y<4}`, &TraceQLScript{
			Head: sel(term(".x", "<", Value{FVal: "3"})), Op: "<",
			Tail: &TraceQLScript{Head: sel(term(".y", "<", Value{FVal: "4"}))},
		}},
		{`{ .x !~ "a" } !~ { .y =~ "b" }`, &TraceQLScript{
			Head: sel(term(".x", "!~", str(`"a"`))), Op: "!~",
			Tail: &TraceQLScript{Head: sel(term(".y", "=~", str(`"b"`)))},
		}},
		{`{ .x >= 1 } &> { .y <= 2 }`, &TraceQLScript{
			Head: sel(term(".x", ">=", Value{FVal: "1"})), Op: "&>",
			Tail: &TraceQLScript{Head: sel(term(".y", "<=", Value{FVal: "2"}))},
		}},
		{`{ .x = "<<&" } > {}`, &TraceQLScript{
			Head: sel(term(".x", "=", str(`"<<&"`))), Op: ">", Tail: &TraceQLScript{},
		}},
	} {
		t.Run(c.query, func(t *testing.T) {
			got, err := Parse(c.query)
			require.NoError(t, err)
			require.Equal(t, c.want, got)
		})
	}

	for _, q := range []string{
		// Transitive operators are not accepted yet.
		`{.a = "b"} >> {.c = "d"}`,
		`{.a = "b"} << {.c = "d"}`,
		`{.a = "b"} &<< {.c = "d"}`,
		`{.a = "b"} !<< {.c = "d"}`,
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
		// Structural operators inside a selector.
		`{.a > {.c = "d"}}`,
		`{.a &> 1}`,
	} {
		t.Run("rejects "+q, func(t *testing.T) {
			_, err := Parse(q)
			require.Error(t, err)
		})
	}

	for op, tempo := range map[string]string{"<<&": "&<<", "<<~": "!<<"} {
		t.Run("rejects legacy "+op, func(t *testing.T) {
			_, err := Parse(`{.a = "b"} ` + op + ` {.c = "d"}`)
			require.EqualError(t, err, op+" is not a TraceQL operator, use "+tempo)
		})
	}
}

func TestParseAggregators(t *testing.T) {
	for _, c := range []struct {
		query string
		head  *AttrSelectorExp
		want  *Aggregator
	}{
		{`{.a = "b"} | count() > 2`, term(".a", "=", str(`"b"`)),
			&Aggregator{Fn: "count", Cmp: ">", Num: "2"}},
		{`{} | count() = 3.5`, nil, &Aggregator{Fn: "count", Cmp: "=", Num: "3.5"}},
		{`{} | avg(duration) >= 1.5ms`, nil,
			&Aggregator{Fn: "avg", Attr: "duration", Cmp: ">=", Num: "1.5", Measurement: "ms"}},
		{`{} | max(span.x) < -3`, nil, &Aggregator{Fn: "max", Attr: "span.x", Cmp: "<", Num: "-3"}},
		{`{} | min(.x) != 0`, nil, &Aggregator{Fn: "min", Attr: ".x", Cmp: "!=", Num: "0"}},
		{`{} | sum(.x) <= 10s`, nil, &Aggregator{Fn: "sum", Attr: ".x", Cmp: "<=", Num: "10", Measurement: "s"}},
	} {
		t.Run(c.query, func(t *testing.T) {
			got, err := Parse(c.query)
			require.NoError(t, err)
			require.Equal(t, &TraceQLScript{Head: Selector{AttrSelector: c.head, Aggregator: c.want}}, got)
		})
	}

	t.Run("binds to the selector before it", func(t *testing.T) {
		got, err := Parse(`{} && {} | count() > 1`)
		require.NoError(t, err)
		require.Equal(t, &TraceQLScript{
			Op:   "&&",
			Tail: &TraceQLScript{Head: Selector{Aggregator: &Aggregator{Fn: "count", Cmp: ">", Num: "1"}}},
		}, got)
	})

	for _, q := range []string{
		`{} | count() > "2"`,
		`{} | median(duration) > 2`,
		`{} | count() > 2 | count() > 3`,
	} {
		t.Run("rejects "+q, func(t *testing.T) {
			_, err := Parse(q)
			require.Error(t, err)
		})
	}
}

func TestParseMetricsFunctions(t *testing.T) {
	for _, c := range []struct {
		query string
		want  *TraceQLScript
	}{
		{`{.a = "b"} | rate()`, &TraceQLScript{Head: Selector{
			AttrSelector: term(".a", "=", str(`"b"`)),
			MetricsFn:    &MetricsPipelineStage{Fn: "rate"},
		}}},
		{`{} | count_over_time() by (span.x, name)`, &TraceQLScript{Head: Selector{
			MetricsFn: &MetricsPipelineStage{Fn: "count_over_time", By: &ByClause{Labels: []string{"span.x", "name"}}},
		}}},
		{`{} | quantile_over_time(duration, 0.9)`, &TraceQLScript{Head: Selector{
			MetricsFn: &MetricsPipelineStage{Fn: "quantile_over_time", Attr: "duration", Percentile: f64(0.9)},
		}}},
		{`{} | histogram_over_time(duration)`, &TraceQLScript{Head: Selector{
			MetricsFn: &MetricsPipelineStage{Fn: "histogram_over_time", Attr: "duration"},
		}}},
		{`{} | compare({.a = "b"}, 10, 1, 2)`, &TraceQLScript{Head: Selector{
			MetricsFn: &MetricsPipelineStage{Fn: "compare", CompSel: &CompareSelector{
				AttrSelector: term(".a", "=", str(`"b"`)), Count: 10, BaselineFrom: i64(1), BaselineTo: i64(2),
			}},
		}}},
		{`{} | compare({})`, &TraceQLScript{Head: Selector{
			MetricsFn: &MetricsPipelineStage{Fn: "compare", CompSel: &CompareSelector{}},
		}}},
		{`{.a = "b"} | count() > 2 | rate()`, &TraceQLScript{Head: Selector{
			AttrSelector: term(".a", "=", str(`"b"`)),
			Aggregator:   &Aggregator{Fn: "count", Cmp: ">", Num: "2"},
			MetricsFn:    &MetricsPipelineStage{Fn: "rate"},
		}}},
		{`({} &>> {}) | rate()`, &TraceQLScript{
			ParenExpr: &TraceQLScript{Op: "&>>", Tail: &TraceQLScript{}},
			MetricsFn: &MetricsPipelineStage{Fn: "rate"},
		}},
		{`{} | by(.a)`, &TraceQLScript{Head: Selector{
			MetricsFn: &MetricsPipelineStage{Fn: "by", Attr: ".a"},
		}}},
		{`{} | coalesce()`, &TraceQLScript{Head: Selector{
			MetricsFn: &MetricsPipelineStage{Fn: "coalesce"},
		}}},
		{`{} | select(.a)`, &TraceQLScript{Head: Selector{
			MetricsFn: &MetricsPipelineStage{Fn: "select", Attr: ".a"},
		}}},
		{`{} && {} | rate()`, &TraceQLScript{
			Op:   "&&",
			Tail: &TraceQLScript{Head: Selector{MetricsFn: &MetricsPipelineStage{Fn: "rate"}}},
		}},
	} {
		t.Run(c.query, func(t *testing.T) {
			got, err := Parse(c.query)
			require.NoError(t, err)
			require.Equal(t, c.want, got)
		})
	}
}

func TestParseWithHintsAndSecondStage(t *testing.T) {
	rate := Selector{MetricsFn: &MetricsPipelineStage{Fn: "rate"}}
	for _, c := range []struct {
		query string
		want  *TraceQLScript
	}{
		{`{} | rate() with(sample=0.1, exemplars=false)`, &TraceQLScript{Head: rate,
			WithHints: &WithClause{Hints: []WithHint{{Key: "sample", Value: "0.1"}, {Key: "exemplars", Value: "false"}}},
		}},
		{`{} | rate() with(sample=true, exemplars=5, most_recent=x)`, &TraceQLScript{Head: rate,
			WithHints: &WithClause{Hints: []WithHint{
				{Key: "sample", Value: "true"}, {Key: "exemplars", Value: "5"}, {Key: "most_recent", Value: "x"},
			}},
		}},
		{`{} | rate() | topk(5)`, &TraceQLScript{Head: rate,
			SecondStage: &SecondPipelineStage{Fn: "topk", Count: 5},
		}},
		{`{} | rate() | bottomk(3) with(sample=0.5)`, &TraceQLScript{Head: rate,
			SecondStage: &SecondPipelineStage{Fn: "bottomk", Count: 3,
				WithHints: &WithClause{Hints: []WithHint{{Key: "sample", Value: "0.5"}}}},
		}},
		{`{} | rate() | select(span.a, duration)`, &TraceQLScript{Head: rate,
			SecondStage: &SecondPipelineStage{Fn: "select", Labels: []string{"span.a", "duration"}},
		}},
		{`{} | rate() with(sample=0.1) | topk(1)`, &TraceQLScript{Head: rate,
			WithHints:   &WithClause{Hints: []WithHint{{Key: "sample", Value: "0.1"}}},
			SecondStage: &SecondPipelineStage{Fn: "topk", Count: 1},
		}},
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
		`{} | {}`,
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
