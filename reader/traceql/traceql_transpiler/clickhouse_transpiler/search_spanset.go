package clickhouse_transpiler

import (
	"fmt"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	sql "github.com/metrico/qryn/v5/reader/utils/sql_select"
)

// DefaultSpansPerSpanSet is the spanset span cap when the request sets none.
const DefaultSpansPerSpanSet = 3

// SearchSpansetPlanner groups the result span rows by trace. Each trace lists
// its newest spss spans (ties by span id), the count of all of them as matched
// and its spanset attributes. Traces come newest-span first.
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
			sql.NewSimpleCol("toInt64(count(DISTINCT result_spans.span_id))", "matched"),
			sql.NewSimpleCol("any(result_spans.attrs)", "attrs")).
		From(sql.NewWithRef(withSpans)).
		GroupBy(sql.NewRawObject("trace_id")).
		OrderBy(
			sql.NewOrderBy(sql.NewRawObject("max(timestamp_ns)"), sql.ORDER_BY_DIRECTION_DESC),
			sql.NewOrderBy(sql.NewRawObject("trace_id"), sql.ORDER_BY_DIRECTION_DESC))
	return res, nil
}
