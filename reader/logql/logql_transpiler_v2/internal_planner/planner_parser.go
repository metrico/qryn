package internal_planner

import (
	"fmt"
	"github.com/metrico/qryn/reader/logql/logql_transpiler_v2/shared"
)

type ParserPlanner struct {
	GenericPlanner
	Op              string
	ParameterNames  []string
	ParameterValues []string

	parameterTypedValues [][]any
	logfmtFields         map[string]string
}

func (p *ParserPlanner) IsMatrix() bool { return false }

func (p *ParserPlanner) Process(ctx *shared.PlannerContext,
	in chan []shared.LogEntry) (chan []shared.LogEntry, error) {

	p.parameterTypedValues = make([][]any, len(p.ParameterValues))
	for i, v := range p.ParameterValues {
		var err error
		p.parameterTypedValues[i], err = shared.JsonPathParamToTypedArray(v)
		if err != nil {
			return nil, err
		}
	}

	if len(p.ParameterNames) > 0 {
		p.logfmtFields = make(map[string]string, len(p.ParameterNames))
		for i, name := range p.ParameterNames {
			if len(p.parameterTypedValues[i]) == 0 {
				continue
			}
			switch p.parameterTypedValues[i][0].(type) {
			case string:
				p.logfmtFields[p.parameterTypedValues[i][0].(string)] = name
			}
		}
	}

	parser := p.json
	switch p.Op {
	case "json":
		if len(p.ParameterNames) > 0 {
			parser = p.jsonWithParams
		}
		break
	case "logfmt":
		parser = p.logfmt
	default:
		return nil, &shared.NotSupportedError{Msg: fmt.Sprintf("%s not supported", p.Op)}
	}

	return p.WrapProcess(ctx, in, GenericPlannerOps{
		OnEntry: func(entry *shared.LogEntry) error {
			if entry.Err != nil {
				return nil
			}
			var err error
			entry.Labels, err = parser(entry.Message, &entry.Labels)
			entry.Fingerprint = fingerprint(entry.Labels)
			return err
		},
		OnAfterEntriesSlice: func(entries []shared.LogEntry, c chan []shared.LogEntry) error {
			c <- entries
			return nil
		},
		OnAfterEntries: func(c chan []shared.LogEntry) error {
			return nil
		},
	})
}
