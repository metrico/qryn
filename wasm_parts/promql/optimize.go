package promql

import (
	"fmt"
	"github.com/prometheus/prometheus/promql/parser"
)

func PlanOptimize(node parser.Node, optimizer IOptimizer) (IOptimizer, error) {
	err := optimizer.PlanOptimize(node)
	if err != nil {
		return nil, err
	}

	var checkLabelAliases func(opt IOptimizer, i int) int
	checkLabelAliases = func(opt IOptimizer, i int) int {
		var _i int
		for _, c := range opt.Children() {
			_i = checkLabelAliases(c, i)
		}
		switch opt.(type) {
		case *AggregateOptimizer:
			if _i != 0 {
				opt.(*AggregateOptimizer).WithLabelsIn = fmt.Sprintf("labels_", _i)
			}
			opt.(*AggregateOptimizer).WithLabelsOut = fmt.Sprintf("labels_%d", _i+1)
			_i++
		case *FinalizerOptimizer:
			if _i != 0 {
				opt.(*FinalizerOptimizer).LabelsIn = fmt.Sprintf("labels_%d", _i)
			}
			_i++
		}
		return _i
	}
	checkLabelAliases(optimizer, 0)
	return optimizer, nil
}
