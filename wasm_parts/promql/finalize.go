package promql

import (
	"github.com/prometheus/prometheus/promql/parser"
	"wasm_parts/promql/planners"
	"wasm_parts/promql/shared"
)

type FinalizerOptimizer struct {
	LabelsIn     string
	SubOptimizer IOptimizer
}

func (f *FinalizerOptimizer) IsAppliable(node parser.Node) bool {
	return false
}

func (f *FinalizerOptimizer) Optimize(node parser.Node) (shared.RequestPlanner, error) {
	planner, err := f.SubOptimizer.Optimize(node)
	if err != nil {
		return nil, err
	}
	labelsIn := f.LabelsIn
	if labelsIn == "" {
		planner = &planners.LabelsInitPlanner{
			Main:              planner,
			FingerprintsAlias: "fp_sel",
		}
		labelsIn = "labels"
	}

	planner = &planners.FinalizePlanner{
		LabelsAlias: labelsIn,
		Main:        planner,
	}
	return planner, nil
}

func (f *FinalizerOptimizer) PlanOptimize(node parser.Node) error {
	return f.SubOptimizer.PlanOptimize(node)
}

func (f *FinalizerOptimizer) Children() []IOptimizer {
	return []IOptimizer{f.SubOptimizer}
}
