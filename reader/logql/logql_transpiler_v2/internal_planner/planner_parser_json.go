package internal_planner

import (
	"fmt"
	"github.com/go-faster/jx"
	_ "github.com/go-faster/jx"
	"regexp"
)

func (p *ParserPlanner) json(str string, labels *map[string]string) (map[string]string, error) {
	dec := jx.DecodeStr(str)
	if dec.Next() != jx.Object {
		return nil, fmt.Errorf("not an object")
	}
	err := p.subDec(dec, "", labels)
	return *labels, err
}

func (p *ParserPlanner) subDec(dec *jx.Decoder, prefix string, labels *map[string]string) error {
	return dec.Obj(func(d *jx.Decoder, key string) error {
		_prefix := prefix
		if _prefix != "" {
			_prefix += "_"
		}
		_prefix += key
		switch d.Next() {
		case jx.Object:
			return p.subDec(d, _prefix, labels)
		case jx.String:
			val, err := d.Str()
			if err != nil {
				return err
			}
			(*labels)[sanitizeLabel(_prefix)] = val
			return nil
		case jx.Array:
			return d.Skip()
		default:
			raw, err := d.Raw()
			if err != nil {
				return err
			}
			(*labels)[sanitizeLabel(_prefix)] = raw.String()
			return nil
		}
	})
}

type pathAhead struct {
	label string
	path  []any
}

type jsonPathProcessor struct {
	labels *map[string]string
}

func (p *ParserPlanner) jsonWithParams(str string, labels *map[string]string) (map[string]string, error) {
	dec := jx.DecodeStr(str)
	var pa []pathAhead
	for i, path := range p.parameterTypedValues {
		name := p.ParameterNames[i]
		pa = append(pa, pathAhead{label: name, path: path})
	}
	jpp := &jsonPathProcessor{labels: labels}
	err := jpp.process(dec, pa)
	if err != nil {
		return nil, err
	}
	return *jpp.labels, nil
}

func (j *jsonPathProcessor) process(dec *jx.Decoder, aheads []pathAhead) error {
	switch dec.Next() {
	case jx.Object:
		return j.processObject(dec, aheads)
	case jx.Array:
		return j.processArray(dec, aheads)
	case jx.String:
		val, err := dec.Str()
		if err != nil {
			return err
		}
		for _, a := range aheads {
			if len(a.path) == 0 {
				(*j.labels)[a.label] = val
			}
		}
	default:
		raw, err := dec.Raw()
		if err != nil {
			return err
		}
		val := raw.String()
		for _, a := range aheads {
			if len(a.path) == 0 {
				(*j.labels)[a.label] = val
			}
		}
	}
	return nil
}

func (j *jsonPathProcessor) processObject(dec *jx.Decoder, aheads []pathAhead) error {
	if len(aheads) == 0 {
		return dec.Skip()
	}
	return dec.Obj(func(d *jx.Decoder, key string) error {
		_aheads := filterAhead(key, aheads)
		if len(_aheads) == 0 {
			return dec.Skip()
		}
		var __aheads []pathAhead
		for _, a := range _aheads {
			__aheads = append(__aheads, pathAhead{label: a.label, path: a.path[1:]})
		}
		return j.process(d, __aheads)
	})
}

func (j *jsonPathProcessor) processArray(dec *jx.Decoder, aheads []pathAhead) error {
	if len(aheads) == 0 {
		return dec.Skip()
	}
	i := -1
	return dec.Arr(func(d *jx.Decoder) error {
		i++
		_aheads := filterAhead(i, aheads)
		if len(_aheads) == 0 {
			return dec.Skip()
		}
		var __aheads []pathAhead
		for _, a := range _aheads {
			__aheads = append(__aheads, pathAhead{label: a.label, path: a.path[1:]})
		}
		return j.process(dec, __aheads)
	})
}

func typeCmp[T int | string](a any, b any) bool {
	_a, ok1 := a.(T)
	_b, ok2 := b.(T)
	if !ok1 || !ok2 {
		return false
	}
	return _a == _b
}

func filterAhead(key any, aheads []pathAhead) []pathAhead {
	var result []pathAhead
	for _, a := range aheads {
		if len(a.path) == 0 {
			continue
		}
		if typeCmp[int](a.path[0], key) || typeCmp[string](a.path[0], key) {
			result = append(result, a)
		}
	}
	return result
}

var sanitizeRe = regexp.MustCompile("[^a-zA-Z0-9_]")

func sanitizeLabel(label string) string {
	return sanitizeRe.ReplaceAllString(label, "_")
}
