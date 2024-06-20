package promql

import (
	"github.com/prometheus/prometheus/promql/parser"
	"wasm_parts/promql/shared"
)

type IOptimizer interface {
	IsAppliable(node parser.Node) bool
	Optimize(node parser.Node) (shared.RequestPlanner, error)
	PlanOptimize(node parser.Node) error
	Children() []IOptimizer
}

type OptimizerFactory func() IOptimizer

var VectorSelectorOptimizerFactory OptimizerFactory = func() IOptimizer {
	return &VectorSelectorOptimizer{}
}

var FinalizerOptimizerFactory OptimizerFactory = func() IOptimizer {
	return &FinalizerOptimizer{}
}

var Optimizers = []OptimizerFactory{
	func() IOptimizer {
		return &RateOptimizer{}
	},
	func() IOptimizer {
		return &AggregateOptimizer{}
	},
}

func GetAppliableOptimizer(node parser.Node, factories []OptimizerFactory) IOptimizer {
	if factories == nil {
		factories = Optimizers
	}
	for _, factory := range factories {
		opt := factory()
		if opt.IsAppliable(node) {
			return opt
		}
	}
	return nil
}
