package clickhouse_transpiler

import (
	"fmt"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	sql "github.com/metrico/qryn/v5/reader/utils/sql_select"
)

// DefaultSpansPerSpanSet is the spanset span cap when the request sets none.
const DefaultSpansPerSpanSet = 3

// SearchSpansetPlanner groups the evaluated result spans by trace. Each trace
// lists its newest spss spans (ties by span id) and the count of all of them
// as matched. Traces come newest-span first.
type SearchSpansetPlanner struct {
	Main shared.SQLRequestPlanner
}

func (s *SearchSpansetPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	main, err := s.Main.Process(ctx)
	if err != nil {
		return nil, err
	}
	spss := ctx.SpansPerSpanSet
	if spss <= 0 {
		spss = DefaultSpansPerSpanSet
	}

	withSpans := sql.NewWith(main, "result_spans")
	res := sql.NewSelect().
		With(withSpans).
		Select(
			sql.NewSimpleCol("trace_id", "trace_id"),
			sql.NewSimpleCol(fmt.Sprintf(
				"arrayMap(x -> x.2, arraySlice(arrayReverseSort(groupUniqArray((result_spans.timestamp_ns, result_spans.span_id))), 1, %d))", spss),
				"span_id"),
			sql.NewSimpleCol("toInt64(count(DISTINCT result_spans.span_id))", "matched")).
		From(sql.NewWithRef(withSpans)).
		GroupBy(sql.NewRawObject("trace_id")).
		OrderBy(
			sql.NewOrderBy(sql.NewRawObject("max(timestamp_ns)"), sql.ORDER_BY_DIRECTION_DESC),
			sql.NewOrderBy(sql.NewRawObject("trace_id"), sql.ORDER_BY_DIRECTION_DESC))

	if cond := intrinsicFilter(ctx, withSpans); cond != nil {
		res.AndWhere(cond)
	}
	return res, nil
}

// intrinsicFilter keeps the rows of spans, evaluated span rows, that pass the
// traces-table conditions the expression's selectors set, or is nil without any.
func intrinsicFilter(ctx *shared.PlannerContext, spans *sql.With) sql.SQLCondition {
	if len(ctx.TracesIntrinsicWhere) == 0 {
		return nil
	}
	intrinsic := sql.NewSelect().
		Select(sql.NewRawObject("traces.trace_id"), sql.NewRawObject("traces.span_id")).
		From(sql.NewSimpleCol(tracesTable(ctx), "traces")).
		AndWhere(inSubquery(ctx, sql.NewRawObject("traces.trace_id"),
			sql.NewSelect().Select(sql.NewRawObject("trace_id")).From(sql.NewWithRef(spans)))).
		AndWhere(ctx.TracesIntrinsicWhere...)
	alias := spans.GetAlias()
	return sql.NewIn(sql.NewRawObject(fmt.Sprintf("(%s.trace_id, %s.span_id)", alias, alias)), intrinsic)
}
