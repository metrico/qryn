// Package timerange holds the query time-range predicates for TraceQL search,
// autocomplete and metrics.
package timerange

import (
	sql "github.com/metrico/qryn/v5/reader/utils/sql_select"
)

// SpanStart returns the lower and upper bounds restricting col, a span start
// time in ns, to [fromNs, toNs).
func SpanStart(col string, fromNs, toNs int64) (lower, upper sql.SQLCondition) {
	c := sql.NewRawObject(col)
	return sql.Ge(c, sql.NewIntVal(fromNs)), sql.Lt(c, sql.NewIntVal(toNs))
}

// TracePick returns the bounds for the attribute-less trace pre-pick, which
// restricts col to [fromNs, toNs].
func TracePick(col string, fromNs, toNs int64) (lower, upper sql.SQLCondition) {
	c := sql.NewRawObject(col)
	return sql.Ge(c, sql.NewIntVal(fromNs)), sql.Le(c, sql.NewIntVal(toNs))
}
