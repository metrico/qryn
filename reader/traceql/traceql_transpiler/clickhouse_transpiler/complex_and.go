package clickhouse_transpiler

import (
	"fmt"
	"strings"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	sql "github.com/metrico/qryn/v5/reader/utils/sql_select"
)

// ComplexAndPlanner keeps the spans every operand returns, in traces where
// every operand's newest span starts at the same time.
type ComplexAndPlanner struct {
	Operands []shared.SQLRequestPlanner
	Prefix   string
}

func (c ComplexAndPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	selects := make([]sql.ISelect, len(c.Operands))
	for i, op := range c.Operands {
		main, err := op.Process(ctx)
		if err != nil {
			return nil, err
		}
		with := sql.NewWith(main, fmt.Sprintf("_%d_pre_", i))
		selects[i] = sql.NewSelect().
			With(with).
			Select(sql.NewSimpleCol("trace_id", "trace_id"),
				sql.NewSimpleCol("span_id", "span_id"),
				sql.NewSimpleCol("timestamp_ns", "timestamp_ns"),
				sql.NewSimpleCol("max(timestamp_ns) OVER (PARTITION BY trace_id)", "max_timestamp_ns")).
			From(sql.NewWithRef(with))
	}

	return sql.NewSelect().
		Select(sql.NewSimpleCol("trace_id", "trace_id"),
			sql.NewSimpleCol("span_id", "span_id"),
			sql.NewSimpleCol("timestamp_ns", "timestamp_ns")).
		From(sql.NewCol(&intersect{
			selects: selects,
		}, c.Prefix+"a")), nil
}

type intersect struct {
	sql.ISelect
	selects []sql.ISelect
}

func (i *intersect) String(ctx *sql.Ctx, opts ...int) (string, error) {
	var _opts []int
	for _, opt := range opts {
		if opt != sql.STRING_OPT_SKIP_WITH {
			_opts = append(_opts, opt)
		}
	}
	strSelects := make([]string, len(i.selects))
	var err error
	for i, s := range i.selects {
		strSelects[i], err = s.String(ctx, _opts...)
		if err != nil {
			return "", err
		}
	}
	return fmt.Sprintf("(%s)", strings.Join(strSelects, " INTERSECT ")), nil
}
