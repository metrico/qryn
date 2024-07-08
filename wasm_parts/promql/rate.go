package promql

import (
	"github.com/prometheus/prometheus/promql/parser"
	"wasm_parts/promql/planners"
	"wasm_parts/promql/shared"
)

type RateOptimizer struct {
	vectorSelectorOptimizer *VectorSelectorOptimizer
}

func (r *RateOptimizer) IsAppliable(node parser.Node) bool {
	_node, ok := node.(*parser.Call)
	if !ok {
		return false
	}
	vectorSelector := r.getVectorSelector(_node)
	return vectorSelector != nil && (&VectorSelectorOptimizer{}).IsAppliable(vectorSelector)
}

func (r *RateOptimizer) Optimize(node parser.Node) (shared.RequestPlanner, error) {
	_node, ok := node.(*parser.Call)
	if !ok {
		return nil, nil
	}
	vectorSelector := r.getVectorSelector(_node)
	matrixSelector := _node.Args[0].(*parser.MatrixSelector)
	res, err := (&VectorSelectorOptimizer{}).Optimize(vectorSelector)
	if err != nil {
		return nil, err
	}
	res = &planners.RatePlanner{
		Main:     res,
		Duration: matrixSelector.Range,
	}
	return res, nil
}

func (v *RateOptimizer) PlanOptimize(node parser.Node) error {
	v.vectorSelectorOptimizer = &VectorSelectorOptimizer{}
	return v.vectorSelectorOptimizer.PlanOptimize(node)
}

func (r *RateOptimizer) getVectorSelector(node *parser.Call) *parser.VectorSelector {
	if node.Func.Name != "rate" || len(node.Args) != 1 {
		return nil
	}
	_matrixSelector, ok := node.Args[0].(*parser.MatrixSelector)
	if !ok {
		return nil
	}
	vectorSelector, ok := _matrixSelector.VectorSelector.(*parser.VectorSelector)
	if !ok {
		return nil
	}
	return vectorSelector
}

func (r *RateOptimizer) Children() []IOptimizer {
	return []IOptimizer{r.vectorSelectorOptimizer}
}
