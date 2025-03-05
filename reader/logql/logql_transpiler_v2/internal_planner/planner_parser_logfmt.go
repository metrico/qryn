package internal_planner

import (
	"github.com/kr/logfmt"
)

func (p *ParserPlanner) logfmt(str string, labels *map[string]string) (map[string]string, error) {
	err := logfmt.Unmarshal([]byte(str), &logFmtParser{labels: labels, fields: p.logfmtFields})
	return *labels, err
}

type logFmtParser struct {
	labels *map[string]string
	fields map[string]string
}

func (p *logFmtParser) HandleLogfmt(key, val []byte) error {
	if p.fields != nil {
		l := p.fields[string(key)]
		if l != "" {
			(*p.labels)[l] = string(val)
		}
		return nil
	}
	(*p.labels)[sanitizeLabel(string(key))] = string(val)
	return nil
}
