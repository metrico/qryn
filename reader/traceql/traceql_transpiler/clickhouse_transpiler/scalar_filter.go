package clickhouse_transpiler

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	"github.com/metrico/qryn/v5/reader/traceql/timerange"
	traceql_ir "github.com/metrico/qryn/v5/reader/traceql/traceql_ir"
	sql "github.com/metrico/qryn/v5/reader/utils/sql_select"
)

// The texts are verbatim, "%v" included.
var (
	errScalarFilterLHS = errors.New("scalar filter lhs of type (%v) not yet supported")
	errScalarFilterRHS = errors.New("scalar filter rhs of type (%v) not yet supported")
)

// nonNumericIntrinsics are the intrinsics an aggregate cannot take.
var nonNumericIntrinsics = map[string]bool{
	"name": true, "span:name": true, "status": true, "span:status": true, "kind": true, "span:kind": true,
	"statusMessage": true, "span:statusMessage": true, "rootName": true, "rootServiceName": true,
	"trace:rootName": true, "trace:rootService": true, "true": true, "false": true, "nil": true,
}

// aggregateArg is the kind of value an aggregate reads per span; count()
// reads none.
type aggregateArg int

const (
	argNone aggregateArg = iota
	argDuration
	argAttribute
)

// scalarFilterStage keeps the spansets whose aggregate over their spans passes
// `Agg Op Value` and appends the aggregate to their attributes.
type scalarFilterStage struct {
	Agg   *traceql_ir.Aggregate
	Op    string
	Value float64
	arg   aggregateArg
	key   string
}

// newScalarFilterStage validates f: an aggregate over a number compared with
// a number or duration literal. Both sides compare as floats, durations in
// nanoseconds.
func newScalarFilterStage(f *traceql_ir.ScalarFilter) (*scalarFilterStage, error) {
	for _, side := range []traceql_ir.ScalarExpr{f.LHS, f.RHS} {
		if err := checkAggregates(side); err != nil {
			return nil, err
		}
	}
	agg, ok := f.LHS.(*traceql_ir.Aggregate)
	if !ok {
		return nil, errScalarFilterLHS
	}
	lit, ok := f.RHS.(*traceql_ir.Static)
	if !ok {
		return nil, errScalarFilterRHS
	}
	value, err := staticValue(lit)
	if err != nil {
		return nil, err
	}
	res := &scalarFilterStage{Agg: agg, Op: f.Op, Value: value}
	res.arg, res.key = aggregateArgOf(agg)
	return res, nil
}

// checkAggregates checks that every aggregate of e reads a number.
func checkAggregates(e traceql_ir.ScalarExpr) error {
	switch e := e.(type) {
	case *traceql_ir.ScalarOp:
		if err := checkAggregates(e.LHS); err != nil {
			return err
		}
		return checkAggregates(e.RHS)
	case *traceql_ir.Aggregate:
		if nonNumericIntrinsics[e.Attr] {
			return fmt.Errorf("aggregate field expressions must resolve to a number type: %s", e)
		}
		if arg, _ := aggregateArgOf(e); arg == argNone && e.Fn != "count" {
			return fmt.Errorf("unsupported attribute %s", e.Attr)
		}
	}
	return nil
}

// aggregateArgOf returns what agg reads per span and, for an attribute, its
// unscoped key.
func aggregateArgOf(agg *traceql_ir.Aggregate) (aggregateArg, string) {
	switch agg.Attr {
	case "duration", "span:duration":
		return argDuration, ""
	}
	for _, scope := range []string{"span.", "resource.", "."} {
		if strings.HasPrefix(agg.Attr, scope) {
			return argAttribute, agg.Attr[len(scope):]
		}
	}
	return argNone, ""
}

// staticValue is a literal as a float: a duration in nanoseconds.
func staticValue(lit *traceql_ir.Static) (float64, error) {
	if lit.Kind == traceql_ir.ValueDuration {
		d, err := time.ParseDuration(lit.Text)
		if err != nil {
			return 0, err
		}
		return float64(d.Nanoseconds()), nil
	}
	return strconv.ParseFloat(lit.Text, 64)
}

func (s *scalarFilterStage) process(ctx *shared.PlannerContext, alias string, prev, base *sql.With) (sql.ISelect, error) {
	var (
		withs  []*sql.With
		valued sql.ISelect
	)
	window := "OVER (PARTITION BY trace_id)"
	if s.arg == argNone {
		valued = sql.NewSelect().
			Select(append(spanRowCols(),
				sql.NewSimpleCol("attrs", "prev_attrs"),
				sql.NewSimpleCol("toFloat64(uniqExact(span_id) "+window+")", "value"),
				sql.NewSimpleCol("1", "n_present"),
				sql.NewSimpleCol("'int'", "type"))...).
			From(sql.NewWithRef(prev))
	} else {
		values := sql.NewWith(s.values(ctx, base), alias+"_values")
		withs = append(withs, values)
		p, v := prev.GetAlias(), values.GetAlias()
		window = fmt.Sprintf("OVER (PARTITION BY %s.trace_id)", p)
		value, typ := s.aggregate(v, window)
		valued = sql.NewSelect().
			Select(
				sql.NewSimpleCol(p+".trace_id", "trace_id"),
				sql.NewSimpleCol(p+".span_id", "span_id"),
				sql.NewSimpleCol(p+".timestamp_ns", "timestamp_ns"),
				sql.NewSimpleCol(p+".attrs", "prev_attrs"),
				sql.NewSimpleCol(value, "value"),
				sql.NewSimpleCol(fmt.Sprintf("countIf(%s.present = 1) %s", v, window), "n_present"),
				sql.NewSimpleCol(typ, "type")).
			From(sql.NewWithRef(prev)).
			Join(sql.NewJoin("left", sql.NewWithRef(values), sql.And(
				sql.Eq(sql.NewRawObject(p+".trace_id"), sql.NewRawObject(v+".trace_id")),
				sql.Eq(sql.NewRawObject(p+".span_id"), sql.NewRawObject(v+".span_id")))))
	}
	withValued := sql.NewWith(valued, alias+"_valued")
	withs = append(withs, withValued)
	return sql.NewSelect().
		With(withs...).
		Select(append(spanRowCols(), sql.NewCol(
			sql.NewRawObject(fmt.Sprintf("arrayPushBack(prev_attrs, tuple(%s, type, value))", quoteString(s.Agg.String()))),
			"attrs"))...).
		From(sql.NewWithRef(withValued)).
		AndWhere(rawCond("n_present > 0 AND NOT isNaN(value) AND " + s.comparison("value"))), nil
}

// values reads the number of every span of base that has one: num, is_int
// (whether the stored value is an integer) and present, always 1.
func (s *scalarFilterStage) values(ctx *shared.PlannerContext, base *sql.With) sql.ISelect {
	baseSpans := sql.NewSelect().Select(sql.NewRawObject("trace_id"), sql.NewRawObject("span_id")).From(sql.NewWithRef(base))
	if s.arg == argDuration {
		tsFrom, tsTo := timerange.SpanStart("traces.timestamp_ns", ctx.From.UnixNano(), ctx.To.UnixNano())
		return sql.NewSelect().
			Select(
				sql.NewSimpleCol("traces.trace_id", "trace_id"),
				sql.NewSimpleCol("traces.span_id", "span_id"),
				sql.NewSimpleCol("toFloat64(any(traces.duration_ns))", "num"),
				sql.NewSimpleCol("1", "is_int"),
				sql.NewSimpleCol("1", "present")).
			From(sql.NewSimpleCol(tracesTable(ctx), "traces")).
			AndWhere(tsFrom, tsTo, inSubquery(ctx, sql.NewRawObject("(traces.trace_id, traces.span_id)"), baseSpans)).
			GroupBy(sql.NewRawObject("traces.trace_id"), sql.NewRawObject("traces.span_id"))
	}
	tsFrom, tsTo := timerange.SpanStart("traces_idx.timestamp_ns", ctx.From.UnixNano(), ctx.To.UnixNano())
	return sql.NewSelect().
		Select(
			sql.NewSimpleCol("traces_idx.trace_id", "trace_id"),
			sql.NewSimpleCol("traces_idx.span_id", "span_id"),
			sql.NewSimpleCol("toFloat64(any(traces_idx.val))", "num"),
			sql.NewSimpleCol("isNotNull(toInt64OrNull(any(traces_idx.val)))", "is_int"),
			sql.NewSimpleCol("1", "present")).
		From(sql.NewSimpleCol(attrsTable(ctx), "traces_idx")).
		AndWhere(
			sql.Eq(sql.NewRawObject("traces_idx.key"), sql.NewStringVal(s.key)),
			sql.Ge(sql.NewRawObject("traces_idx.date"), sql.NewStringVal(ctx.From.Format("2006-01-02"))),
			sql.Le(sql.NewRawObject("traces_idx.date"), sql.NewStringVal(ctx.To.Format("2006-01-02"))),
			tsFrom, tsTo,
			rawCond("isNotNull(toFloat64OrNull(traces_idx.val))"),
			inSubquery(ctx, sql.NewRawObject("(traces_idx.trace_id, traces_idx.span_id)"), baseSpans)).
		GroupBy(sql.NewRawObject("traces_idx.trace_id"), sql.NewRawObject("traces_idx.span_id"))
}

// aggregate returns the aggregate over the present values of v per spanset,
// and its type: a duration for a duration argument, else an int when every
// value it depends on is an integer. avg(duration) truncates to whole
// nanoseconds.
func (s *scalarFilterStage) aggregate(v, window string) (value string, typ string) {
	num, present, isInt := v+".num", v+".present = 1", v+".is_int"
	numeric := func(intExpr string) string {
		if s.arg == argDuration {
			return "'duration'"
		}
		return fmt.Sprintf("if(%s %s = 1, 'int', 'float')", intExpr, window)
	}
	switch s.Agg.Fn {
	case "sum":
		return fmt.Sprintf("sumIf(%s, %s) %s", num, present, window), numeric(fmt.Sprintf("minIf(%s, %s)", isInt, present))
	case "min":
		return fmt.Sprintf("minIf(%s, %s) %s", num, present, window), numeric(fmt.Sprintf("argMinIf(%s, %s, %s)", isInt, num, present))
	case "max":
		return fmt.Sprintf("maxIf(%s, %s) %s", num, present, window), numeric(fmt.Sprintf("argMaxIf(%s, %s, %s)", isInt, num, present))
	}
	if s.arg == argDuration {
		return fmt.Sprintf("toFloat64(intDivOrZero(sumIf(toInt64(%s), %s) %s, countIf(%s) %s))", num, present, window, present, window), "'duration'"
	}
	return fmt.Sprintf("avgIf(%s, %s) %s", num, present, window), "'float'"
}

// comparison is `col Op Value`.
func (s *scalarFilterStage) comparison(col string) string {
	return fmt.Sprintf("%s %s %s", col, s.Op, floatLiteral(s.Value))
}

// floatLiteral renders f as a ClickHouse Float64.
func floatLiteral(f float64) string {
	switch {
	case math.IsInf(f, 1):
		return "inf"
	case math.IsInf(f, -1):
		return "-inf"
	}
	return "toFloat64(" + strconv.FormatFloat(f, 'g', -1, 64) + ")"
}

// quoteString renders s as a ClickHouse string literal.
func quoteString(s string) string {
	return "'" + strings.NewReplacer(`\`, `\\`, `'`, `\'`).Replace(s) + "'"
}
