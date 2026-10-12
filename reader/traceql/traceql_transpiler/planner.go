package traceql_transpiler

import (
	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	"github.com/metrico/qryn/v5/reader/model"
	"github.com/metrico/qryn/v5/reader/traceql/traceql_ir"
	"github.com/metrico/qryn/v5/reader/traceql/traceql_transpiler/clickhouse_transpiler"
)

func Plan(query *traceql_ir.Query) (shared.TraceRequestProcessor, error) {
	sqlPlanner, err := clickhouse_transpiler.Plan(query)
	if err != nil {
		return nil, err
	}

	complexityPlanner, err := clickhouse_transpiler.PlanEval(query)
	if err != nil {
		return nil, err
	}

	return &TraceQLComplexityEvaluator[model.TraceInfo]{
		initSqlPlanner:            sqlPlanner,
		simpleRequestProcessor:    &SimpleRequestProcessor{},
		complexRequestProcessor:   &ComplexRequestProcessor{},
		evaluateComplexityPlanner: complexityPlanner,
	}, nil
}

func PlanTagsV2(query *traceql_ir.Query) (shared.GenericTraceRequestProcessor[string], error) {
	if query == nil {
		return &allTagsV2RequestProcessor{}, nil
	}
	res, err := clickhouse_transpiler.PlanTagsV2(query)
	if err != nil {
		return nil, err
	}

	complexityPlanner, err := clickhouse_transpiler.PlanEval(query)
	if err != nil {
		return nil, err
	}

	return &TraceQLComplexityEvaluator[string]{
		initSqlPlanner:            res,
		simpleRequestProcessor:    &SimpleTagsV2RequestProcessor{},
		complexRequestProcessor:   &ComplexTagsV2RequestProcessor{},
		evaluateComplexityPlanner: complexityPlanner,
	}, nil
}

func PlanValuesV2(query *traceql_ir.Query, key string) (shared.GenericTraceRequestProcessor[string], error) {
	if query == nil {
		return &allTagsV2RequestProcessor{}, nil
	}
	res, err := clickhouse_transpiler.PlanValuesV2(query, key)
	if err != nil {
		return nil, err
	}

	complexityPlanner, err := clickhouse_transpiler.PlanEval(query)
	if err != nil {
		return nil, err
	}

	return &TraceQLComplexityEvaluator[string]{
		initSqlPlanner:            res,
		simpleRequestProcessor:    &SimpleTagsV2RequestProcessor{},
		complexRequestProcessor:   &ComplexValuesV2RequestProcessor{},
		evaluateComplexityPlanner: complexityPlanner,
	}, nil
}
