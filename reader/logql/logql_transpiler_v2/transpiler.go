package logql_transpiler_v2

import (
	"github.com/metrico/qryn/reader/logql/logql_parser"
	"github.com/metrico/qryn/reader/logql/logql_transpiler_v2/shared"
)

func Transpile(script string) (shared.RequestProcessorChain, error) {
	oScript, err := logql_parser.Parse(script)
	if err != nil {
		return nil, err
	}
	return Plan(oScript)
}
