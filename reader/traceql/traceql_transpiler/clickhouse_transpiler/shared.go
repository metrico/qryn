package clickhouse_transpiler

import (
	"fmt"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	sql "github.com/metrico/qryn/v5/reader/utils/sql_select"
)

func getComparisonFn(op string) (func(left sql.SQLObject, right sql.SQLObject) *sql.LogicalOp, error) {
	switch op {
	case "=":
		return sql.Eq, nil
	case ">":
		return sql.Gt, nil
	case "<":
		return sql.Lt, nil
	case ">=":
		return sql.Ge, nil
	case "<=":
		return sql.Le, nil
	case "!=":
		return sql.Neq, nil
	}
	return nil, &shared.NotSupportedError{Msg: "not supported operator: " + op}
}

// inSubquery is `left IN (right)`, or GLOBAL IN on a cluster, where right
// reads Distributed tables.
func inSubquery(ctx *shared.PlannerContext, left sql.SQLObject, right sql.SQLObject) sql.SQLCondition {
	if ctx.IsCluster {
		return &globalIn{left: left, right: right}
	}
	return sql.NewIn(left, right)
}

type globalIn struct {
	left  sql.SQLObject
	right sql.SQLObject
}

func (g *globalIn) GetFunction() string { return "GLOBAL IN" }

func (g *globalIn) GetEntity() []sql.SQLObject { return []sql.SQLObject{g.left, g.right} }

func (g *globalIn) String(ctx *sql.Ctx, options ...int) (string, error) {
	l, err := g.left.String(ctx, options...)
	if err != nil {
		return "", err
	}
	r, err := g.right.String(ctx, options...)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s GLOBAL IN (%s)", l, r), nil
}

// tracesTable is the spans table search reads: Distributed on a cluster.
func tracesTable(ctx *shared.PlannerContext) string {
	if ctx.IsCluster {
		return ctx.TracesDistTable
	}
	return ctx.TracesTable
}
