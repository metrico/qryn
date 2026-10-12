package clickhouse_transpiler

import (
	"fmt"
	"strings"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	sql "github.com/metrico/qryn/v5/reader/utils/sql_select"
)

// ComplexOrPlanner returns the spans any operand returns.
type ComplexOrPlanner struct {
	Operands []shared.SQLRequestPlanner
	Prefix   string
}

func (c ComplexOrPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
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
				sql.NewSimpleCol("timestamp_ns", "timestamp_ns")).
			From(sql.NewWithRef(with))
	}

	return sql.NewSelect().
		Distinct(true).
		Select(sql.NewSimpleCol("trace_id", "trace_id"),
			sql.NewSimpleCol("span_id", "span_id"),
			sql.NewSimpleCol("timestamp_ns", "timestamp_ns")).
		From(sql.NewCol(&union{
			selects: selects,
		}, c.Prefix+"a")), nil
}

type union struct {
	selects []sql.ISelect
}

func (i *union) String(ctx *sql.Ctx, opts ...int) (string, error) {
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
	return fmt.Sprintf("(%s)", strings.Join(strSelects, " UNION ALL ")), nil
}
