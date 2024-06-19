package promql

import (
	"github.com/prometheus/prometheus/promql/parser"
	"wasm_parts/promql/planners"
	"wasm_parts/promql/shared"
)

type VectorSelectorOptimizer struct {
}

func (v *VectorSelectorOptimizer) IsAppliable(node parser.Node) bool {
	_, ok := node.(*parser.VectorSelector)
	return ok
}

func (v *VectorSelectorOptimizer) PlanOptimize(node parser.Node) error {
	return nil
}

func (v *VectorSelectorOptimizer) Optimize(node parser.Node) (shared.RequestPlanner, error) {
	_node := node.(*parser.VectorSelector)
	var res shared.RequestPlanner = &planners.TimeSeriesGinInitPlanner{}
	res = &planners.StreamSelectPlanner{
		Main:     res,
		Matchers: _node.LabelMatchers,
	}
	res = &planners.MetricsInitPlanner{
		ValueCol:    nil,
		Fingerprint: res,
	}
	res = &planners.MetricsZeroFillPlanner{Main: res}
	res = &planners.MetricsExtendPlanner{Main: res}
	return res, nil
}

func (v *VectorSelectorOptimizer) Children() []IOptimizer {
	return nil
}
