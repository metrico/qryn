package traceql_transpiler

import (
	"encoding/json"
	"math"
	"testing"

	"github.com/metrico/qryn/v5/reader/model"
)

// Spanset attributes serialise as OTLP key-values: ints as decimal strings,
// floats as doubles, durations as Go duration strings, bools as bools and
// strings as their text; nil is the string "nil".
func TestSpanSetAttrsJSON(t *testing.T) {
	attrs := spanSetAttrs(
		[]string{"count()", "avg(.a)", "avg(duration)", "max(span:duration)", "sum(.b)", "sum(.c)",
			"by(.x)", "by(.y = 1)", "by(.z)", "by(.w)"},
		[]string{"int", "float", "duration", "duration", "int", "float", "string", "bool", "nil", "bool"},
		[]float64{3, 2.5, 500225, 2e6, -7, math.Inf(1), 12, 1, 0, 0},
		[]string{"", "", "", "", "", "", "12", "", "", ""})
	got, err := json.Marshal(model.SpanSet{Spans: []model.SpanInfo{}, Matched: 3, Attributes: attrs})
	if err != nil {
		t.Fatal(err)
	}
	want := `{"spans":[],"matched":3,"attributes":[` +
		`{"key":"count()","value":{"intValue":"3"}},` +
		`{"key":"avg(.a)","value":{"doubleValue":2.5}},` +
		`{"key":"avg(duration)","value":{"stringValue":"500.225µs"}},` +
		`{"key":"max(span:duration)","value":{"stringValue":"2ms"}},` +
		`{"key":"sum(.b)","value":{"intValue":"-7"}},` +
		`{"key":"sum(.c)","value":{"doubleValue":"Infinity"}},` +
		`{"key":"by(.x)","value":{"stringValue":"12"}},` +
		`{"key":"by(.y = 1)","value":{"boolValue":true}},` +
		`{"key":"by(.z)","value":{"stringValue":"nil"}},` +
		`{"key":"by(.w)","value":{"boolValue":false}}]}`
	if string(got) != want {
		t.Errorf("got  %s\nwant %s", got, want)
	}
}

func TestSpanSetWithoutAttrsOmitsThem(t *testing.T) {
	got, err := json.Marshal(model.SpanSet{Spans: []model.SpanInfo{}, Attributes: spanSetAttrs(nil, nil, nil, nil)})
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != `{"spans":[],"matched":0}` {
		t.Errorf("got %s", got)
	}
}
