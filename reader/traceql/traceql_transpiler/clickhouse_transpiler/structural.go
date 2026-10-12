package clickhouse_transpiler

import (
	"errors"
	"fmt"
	"strings"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	traceql_ir "github.com/metrico/qryn/v5/reader/traceql/traceql_ir"
	dbversion "github.com/metrico/qryn/v5/reader/utils/dbVersion"
	sql "github.com/metrico/qryn/v5/reader/utils/sql_select"
)

var errStructuralVersion = errors.New(
	"TraceQL structural operators need ClickHouse 24.9 or newer")

// reachRounds is the number of pointer-jumping rounds that resolve root
// reachability: chains up to 2^reachRounds spans deep.
const reachRounds = 20

// zeroSpanID is the parent of a root span.
const zeroSpanID = "toFixedString('', 8)"

// spanResult is which spans a structural operator returns.
type spanResult int

const (
	// resultRHS returns the matched RHS spans.
	resultRHS spanResult = iota
	// resultUnion returns the matched RHS and LHS spans.
	resultUnion
	// resultNot returns the RHS spans without a match.
	resultNot
)

// hopKey is the id a span is related by.
type hopKey int

const (
	keySpan hopKey = iota
	keyParent
)

// oneHop relates a LHS and a RHS span with equal keys; siblings must differ.
type oneHop struct {
	lhsKey, rhsKey hopKey
	sibling        bool
	result         spanResult
}

var oneHopOps = map[traceql_ir.SpansetOperator]oneHop{
	traceql_ir.OpChild:        {lhsKey: keySpan, rhsKey: keyParent, result: resultRHS},
	traceql_ir.OpParent:       {lhsKey: keyParent, rhsKey: keySpan, result: resultRHS},
	traceql_ir.OpSibling:      {lhsKey: keyParent, rhsKey: keyParent, sibling: true, result: resultRHS},
	traceql_ir.OpNotChild:     {lhsKey: keySpan, rhsKey: keyParent, result: resultNot},
	traceql_ir.OpNotParent:    {lhsKey: keyParent, rhsKey: keySpan, result: resultNot},
	traceql_ir.OpNotSibling:   {lhsKey: keyParent, rhsKey: keyParent, sibling: true, result: resultNot},
	traceql_ir.OpUnionChild:   {lhsKey: keySpan, rhsKey: keyParent, result: resultUnion},
	traceql_ir.OpUnionParent:  {lhsKey: keyParent, rhsKey: keySpan, result: resultUnion},
	traceql_ir.OpUnionSibling: {lhsKey: keyParent, rhsKey: keyParent, sibling: true, result: resultUnion},
}

// StructuralQueryPlanner runs Main, a plan with structural operators, on
// servers that support them, with the analyzer they need.
type StructuralQueryPlanner struct {
	Main shared.SQLRequestPlanner
}

func (s *StructuralQueryPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	if !ctx.VersionInfo.HasCapability(dbversion.CapStructuralQueries) {
		return nil, errStructuralVersion
	}
	main, err := s.Main.Process(ctx)
	if err != nil {
		return nil, err
	}
	return main.SetSetting("enable_analyzer", "1"), nil
}

// OneHopPlanner relates the span rows of LHS and RHS by parent or sibling.
// Only spans that reach a root relate.
type OneHopPlanner struct {
	Op     traceql_ir.SpansetOperator
	LHS    shared.SQLRequestPlanner
	RHS    shared.SQLRequestPlanner
	Prefix string
}

func (o *OneHopPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	rel, ok := oneHopOps[o.Op]
	if !ok {
		return nil, fmt.Errorf("%s is not a one-hop operator", o.Op)
	}
	alias := o.Prefix + "hop"
	lhs, rhs, err := processSides(ctx, o.LHS, o.RHS, alias)
	if err != nil {
		return nil, err
	}
	reach := reachableSpans(ctx, alias, lhs, rhs)

	// keyed tags each operand span with its side, reachability and key.
	key := func(k hopKey) string {
		if k == keyParent {
			return fmt.Sprintf("ifNull(r.parent, %s)", zeroSpanID)
		}
		return "o.span_id"
	}
	keyed := sql.NewWith(sql.NewSelect().
		Select(
			sql.NewSimpleCol("o.trace_id", "trace_id"),
			sql.NewSimpleCol("o.span_id", "span_id"),
			sql.NewSimpleCol("o.timestamp_ns", "timestamp_ns"),
			sql.NewSimpleCol("o.side", "side"),
			sql.NewSimpleCol("ifNull(r.reachable, 0)", "reachable"),
			sql.NewSimpleCol(fmt.Sprintf("if(o.side = 1, %s, %s)", key(rel.lhsKey), key(rel.rhsKey)), "key")).
		From(sql.NewCol(&union{selects: []sql.ISelect{
			spanRows(lhs, sql.NewSimpleCol("toUInt8(1)", "side")),
			spanRows(rhs, sql.NewSimpleCol("toUInt8(2)", "side")),
		}}, "o")).
		Join(sql.NewJoin("left", sql.NewCol(sql.NewWithRef(reach), "r"), sameSpan("r.span_id", "o.span_id"))),
		alias+"_keyed")

	// lhs_n and rhs_n count each side's spans per key; self_* discount the span.
	group := "PARTITION BY trace_id, reachable, key"
	cols := append(spanRowCols(),
		sql.NewSimpleCol("side", "side"),
		sql.NewSimpleCol("reachable", "reachable"),
		sql.NewSimpleCol(fmt.Sprintf("countIf(side = 1) OVER (%s)", group), "lhs_n"),
		sql.NewSimpleCol(fmt.Sprintf("countIf(side = 2) OVER (%s)", group), "rhs_n"))
	selfLHS, selfRHS := "0", "0"
	if rel.sibling {
		cols = append(cols,
			sql.NewSimpleCol("max(side = 1) OVER (PARTITION BY trace_id, span_id)", "self_lhs"),
			sql.NewSimpleCol("max(side = 2) OVER (PARTITION BY trace_id, span_id)", "self_rhs"))
		selfLHS, selfRHS = "self_lhs", "self_rhs"
	}
	counted := sql.NewWith(sql.NewSelect().Select(cols...).From(sql.NewWithRef(keyed)), alias+"_counted")

	rhsMatched := fmt.Sprintf("reachable = 1 AND lhs_n > %s", selfLHS)
	lhsMatched := fmt.Sprintf("reachable = 1 AND rhs_n > %s", selfRHS)
	var cond string
	switch rel.result {
	case resultRHS:
		cond = fmt.Sprintf("side = 2 AND %s", rhsMatched)
	case resultUnion:
		cond = fmt.Sprintf("(side = 2 AND %s) OR (side = 1 AND %s)", rhsMatched, lhsMatched)
	case resultNot:
		cond = fmt.Sprintf("side = 2 AND NOT (%s)", rhsMatched)
	}
	return sql.NewSelect().
		With(lhs, rhs).
		AddWith(reach, keyed, counted).
		Distinct(true).
		Select(spanRowCols()...).
		From(sql.NewWithRef(counted)).
		AndWhere(rawCond(cond)), nil
}

// reachableSpans is <alias>_reach: the spans that reach a root, with their
// parent, in traces where lhs and rhs both return spans.
func reachableSpans(ctx *shared.PlannerContext, alias string, lhs, rhs *sql.With) *sql.With {
	traceIds := func(w *sql.With) sql.ISelect {
		return sql.NewSelect().Select(sql.NewRawObject("trace_id")).From(sql.NewWithRef(w))
	}
	// edges holds one parent per span, read with no time bound.
	edges := sql.NewWith(sql.NewSelect().
		Select(
			sql.NewSimpleCol("traces.trace_id", "trace_id"),
			sql.NewSimpleCol("traces.span_id", "span_id"),
			sql.NewSimpleCol("any(toFixedString(substring(traces.parent_id, 1, 8), 8))", "parent")).
		From(sql.NewSimpleCol(tracesTable(ctx), "traces")).
		AndWhere(
			inSubquery(ctx, sql.NewRawObject("traces.trace_id"), traceIds(lhs)),
			inSubquery(ctx, sql.NewRawObject("traces.trace_id"), traceIds(rhs))).
		GroupBy(sql.NewRawObject("traces.trace_id"), sql.NewRawObject("traces.span_id")),
		alias+"_edges")

	spans := sql.NewWith(sql.NewSelect().
		Select(
			sql.NewSimpleCol("trace_id", "trace_id"),
			sql.NewSimpleCol("span_id", "span_id"),
			sql.NewSimpleCol("toUInt8(1)", "found")).
		From(sql.NewWithRef(edges)),
		alias+"_spans")
	// links flags roots and spans whose parent is not in the trace.
	links := sql.NewWith(sql.NewSelect().
		Select(
			sql.NewSimpleCol("e.trace_id", "trace_id"),
			sql.NewSimpleCol("e.span_id", "span_id"),
			sql.NewSimpleCol("e.parent", "parent"),
			sql.NewSimpleCol(fmt.Sprintf("toUInt8(e.parent = %s)", zeroSpanID), "is_root"),
			sql.NewSimpleCol(fmt.Sprintf("toUInt8(e.parent != %s AND ifNull(p.found, 0) = 0)", zeroSpanID), "dangling")).
		From(sql.NewCol(sql.NewWithRef(edges), "e")).
		Join(sql.NewJoin("left", sql.NewCol(sql.NewWithRef(spans), "p"), sameSpan("p.span_id", "e.parent"))),
		alias+"_links")
	flags := sql.NewWith(sql.NewSelect().
		Select(
			sql.NewSimpleCol("trace_id", "trace_id"),
			sql.NewSimpleCol("span_id", "span_id"),
			sql.NewSimpleCol("parent", "parent"),
			sql.NewSimpleCol("is_root", "is_root"),
			sql.NewSimpleCol("dangling", "dangling"),
			sql.NewSimpleCol("max(is_root) OVER (PARTITION BY trace_id)", "has_root"),
			sql.NewSimpleCol("max(dangling) OVER (PARTITION BY trace_id)", "has_dangling")).
		From(sql.NewWithRef(links)),
		alias+"_flags")

	// jump runs on traces with a root and a missing parent: anc is the 2^k-th
	// ancestor (zero past the chain's end), cut marks a missing parent on it.
	jump := sql.NewRecursiveWith(
		sql.NewSelect().
			Select(
				sql.NewSimpleCol("trace_id", "trace_id"),
				sql.NewSimpleCol("span_id", "node"),
				sql.NewSimpleCol(fmt.Sprintf("if(is_root = 1 OR dangling = 1, %s, parent)", zeroSpanID), "anc"),
				sql.NewSimpleCol("dangling", "cut"),
				sql.NewSimpleCol("toUInt32(0)", "k")).
			From(sql.NewWithRef(flags)).
			AndWhere(rawCond("has_root = 1 AND has_dangling = 1")),
		alias+"_jump",
		func(self *sql.WithRef) sql.ISelect {
			return sql.NewSelect().
				Select(
					sql.NewSimpleCol("x.trace_id", "trace_id"),
					sql.NewSimpleCol("x.node", "node"),
					sql.NewSimpleCol(fmt.Sprintf("if(x.anc = %[1]s, %[1]s, ifNull(y.anc, %[1]s))", zeroSpanID), "anc"),
					sql.NewSimpleCol(fmt.Sprintf("toUInt8(x.cut = 1 OR (x.anc != %s AND ifNull(y.cut, 0) = 1))", zeroSpanID), "cut"),
					sql.NewSimpleCol("toUInt32(x.k + 1)", "k")).
				From(sql.NewCol(self, "x")).
				Join(sql.NewJoin("left", sql.NewCol(self, "y"), sameSpan("y.node", "x.anc"))).
				AndWhere(sql.Lt(sql.NewRawObject("x.k"), sql.NewIntVal(reachRounds)))
		})
	// A cut chain, or one unresolved after the last round, reaches no root.
	unreachable := sql.NewWith(sql.NewSelect().
		Select(sql.NewSimpleCol("trace_id", "trace_id"), sql.NewSimpleCol("node", "node")).
		From(sql.NewWithRef(jump)).
		AndWhere(rawCond(fmt.Sprintf("k = %d AND (cut = 1 OR anc != %s)", reachRounds, zeroSpanID))),
		alias+"_unreachable")

	return sql.NewWith(sql.NewSelect().
		With(edges, spans, links, flags, jump, unreachable).
		Select(
			sql.NewSimpleCol("f.trace_id", "trace_id"),
			sql.NewSimpleCol("f.span_id", "span_id"),
			sql.NewSimpleCol("f.parent", "parent"),
			sql.NewSimpleCol("toUInt8(1)", "reachable")).
		From(sql.NewCol(sql.NewWithRef(flags), "f")).
		Join(sql.NewJoin("left anti", sql.NewCol(sql.NewWithRef(unreachable), "u"), sameSpan("u.node", "f.span_id"))).
		AndWhere(rawCond("f.has_root = 1")),
		alias+"_reach")
}

// sameSpan joins two aliased rows on trace id and the given span id columns.
func sameSpan(left, right string) sql.SQLCondition {
	alias := func(col string) string { return strings.SplitN(col, ".", 2)[0] }
	return sql.And(
		sql.Eq(sql.NewRawObject(alias(left)+".trace_id"), sql.NewRawObject(alias(right)+".trace_id")),
		sql.Eq(sql.NewRawObject(left), sql.NewRawObject(right)))
}
