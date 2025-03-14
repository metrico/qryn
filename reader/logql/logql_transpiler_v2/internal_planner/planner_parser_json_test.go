package internal_planner

import (
	"fmt"
	"github.com/go-faster/jx"
	"testing"
)

func TestPPJ(t *testing.T) {
	pathsAndJsons := [][]any{
		{`{"a":"b"}`, []any{"a"}},
		{`{"a":{"b":"c"}}`, []any{"a", "b"}},
		{`{"a":["b","c"]}`, []any{"a", 0}},
		{`{"u": 1, "a":{"b":[2,"d"]}}`, []any{"a", "b", 0}},
		{`{"a":{"e":0, "b":{"c":"d"}}}`, []any{"a", "b", "c"}},
		{`["c","d"]`, []any{0}},
		{`["c","d"]`, []any{1}},
	}
	for _, pj := range pathsAndJsons {
		jpp := &jsonPathProcessor{labels: &map[string]string{}}
		dec := jx.DecodeStr(pj[0].(string))
		err := jpp.process(dec, []pathAhead{{label: "a", path: pj[1].([]any)}})
		if err != nil {
			t.Fatal(err)
		}
		fmt.Println(*jpp.labels)
	}
}
