package promql

import (
	"fmt"
	"github.com/prometheus/prometheus/promql/parser"
	"wasm_parts/promql/planners"
	"wasm_parts/promql/shared"
)

type AggregateOptimizer struct {
	WithLabelsIn  string
	WithLabelsOut string

	subOptimizer IOptimizer
}

func (a *AggregateOptimizer) IsAppliable(node parser.Node) bool {
	aggExpr, ok := node.(*parser.AggregateExpr)
	if !ok {
		return false
	}
	if aggExpr.Op != parser.SUM {
		return false
	}
	return GetAppliableOptimizer(aggExpr.Expr, append(Optimizers, VectorSelectorOptimizerFactory)) != nil
}

func (a *AggregateOptimizer) PlanOptimize(node parser.Node) error {
	aggExpr := node.(*parser.AggregateExpr)
	a.subOptimizer = GetAppliableOptimizer(aggExpr.Expr, append(Optimizers, VectorSelectorOptimizerFactory))
	return a.subOptimizer.PlanOptimize(node)
}

func (a *AggregateOptimizer) Optimize(node parser.Node) (shared.RequestPlanner, error) {
	aggExpr := node.(*parser.AggregateExpr)
	planner, err := a.subOptimizer.Optimize(aggExpr.Expr)
	if err != nil {
		return nil, err
	}
	withLabelsIn := a.WithLabelsIn
	if withLabelsIn == "" {
		planner = &planners.LabelsInitPlanner{
			Main:              planner,
			FingerprintsAlias: "fp_sel",
		}
		withLabelsIn = "labels"
	}
	if a.WithLabelsOut == "" {
		return nil, fmt.Errorf("AggregateOptimizer.WithLabelsOut is empty")
	}
	byWithout := "by"
	if aggExpr.Without {
		byWithout = "without"
	}
	planner = &planners.ByWithoutPlanner{
		Main:                planner,
		FingerprintWithName: withLabelsIn,
		FingerprintsOutName: a.WithLabelsOut,
		ByWithout:           byWithout,
		Labels:              aggExpr.Grouping,
	}
	planner = &planners.SumPlanner{
		Main:        planner,
		LabelsAlias: a.WithLabelsOut,
	}
	return planner, nil
}

func (a *AggregateOptimizer) Children() []IOptimizer {
	return []IOptimizer{a.subOptimizer}
}
