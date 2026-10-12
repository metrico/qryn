package traceql_parser

import (
	"encoding/json"
	"fmt"
	"strings"
)

// TraceQLScript is a pipeline followed by the optional metrics function,
// hints and second stage.
type TraceQLScript struct {
	Pipeline    Pipeline              `@@`
	MetricsFn   *MetricsPipelineStage `@@?`
	WithHints   *WithClause           `@@?`
	SecondStage *SecondPipelineStage  `@@?`
}

func (l TraceQLScript) String() string {
	res := l.Pipeline.String()
	if l.MetricsFn != nil {
		res += " " + l.MetricsFn.String()
	}
	if l.WithHints != nil {
		res += " " + l.WithHints.String()
	}
	if l.SecondStage != nil {
		res += " " + l.SecondStage.String()
	}
	return res
}

// Pipeline is `element ('|' element)*`.
type Pipeline struct {
	Head   PipelineElement   `@@`
	Stages []PipelineElement `( "|" @@ )*`
}

func (p Pipeline) String() string {
	return pipelineString(p.Head.String(), p.Stages)
}

func pipelineString(head string, stages []PipelineElement) string {
	res := head
	for _, s := range stages {
		res += " | " + s.String()
	}
	return res
}

// PipelineElement is a spanset expression or a scalar filter.
type PipelineElement struct {
	Spanset      *SpansetExpr  `  @@`
	ScalarFilter *ScalarFilter `| @@`
}

func (e PipelineElement) String() string {
	if e.ScalarFilter != nil {
		return e.ScalarFilter.String()
	}
	return e.Spanset.String()
}

// SpansetExpr is operands joined by spanset operators, in written order.
type SpansetExpr struct {
	Head Operand            `@@`
	Ops  []SpansetOperation `@@*`
}

func (s SpansetExpr) String() string {
	res := s.Head.String()
	for _, op := range s.Ops {
		res += " " + op.Op + " " + op.Operand.String()
	}
	return res
}

// SpansetOperation is a spanset operator and the operand after it. Gt, Lt and
// Nre are structural between spansets.
type SpansetOperation struct {
	Op      string  `@(Gt|Lt|Sibling|NotChild|NotParent|Nre|UnionChild|UnionParent|UnionSibling|Descendant|Ancestor|NotDescendant|NotAncestor|UnionDescendant|UnionAncestor|And|Or)`
	Operand Operand `@@`
}

// Operand is a selector or a parenthesised pipeline.
type Operand struct {
	Paren    *ParenPipeline `  "(" @@ ")"`
	Selector Selector       `| @@`
}

func (o Operand) String() string {
	if o.Paren != nil {
		return "(" + o.Paren.String() + ")"
	}
	return o.Selector.String()
}

// ParenPipeline is a parenthesised pipeline; it starts with a spanset expression.
type ParenPipeline struct {
	Head   SpansetExpr       `@@`
	Stages []PipelineElement `( "|" @@ )*`
}

func (p ParenPipeline) String() string {
	return pipelineString(p.Head.String(), p.Stages)
}

type Selector struct {
	AttrSelector *AttrSelectorExp `"{" @@? "}"`
}

func (s Selector) String() string {
	if s.AttrSelector == nil {
		return "{}"
	}
	return "{" + s.AttrSelector.String() + "}"
}

// ScalarFilter is `LHS Op RHS` over scalar expressions.
type ScalarFilter struct {
	LHS ScalarExpr `@@`
	Op  string     `@("="|"!="|"<"|"<="|">"|">=")`
	RHS ScalarExpr `@@`
}

func (f ScalarFilter) String() string {
	return f.LHS.String() + " " + f.Op + " " + f.RHS.String()
}

// ScalarExpr is `term (('+'|'-') term)*`.
type ScalarExpr struct {
	Head ScalarTerm    `@@`
	Ops  []ScalarSumOp `@@*`
}

func (e ScalarExpr) String() string {
	res := e.Head.String()
	for _, op := range e.Ops {
		res += " " + op.Op + " " + op.Term.String()
	}
	return res
}

type ScalarSumOp struct {
	Op   string     `@(Plus|Minus)`
	Term ScalarTerm `@@`
}

// ScalarTerm is `factor (('*'|'/'|'%') factor)*`.
type ScalarTerm struct {
	Head ScalarFactor  `@@`
	Ops  []ScalarMulOp `@@*`
}

func (t ScalarTerm) String() string {
	res := t.Head.String()
	for _, op := range t.Ops {
		res += " " + op.Op + " " + op.Factor.String()
	}
	return res
}

type ScalarMulOp struct {
	Op     string       `@(Mul|Div|Mod)`
	Factor ScalarFactor `@@`
}

// ScalarFactor is `atom ('^' factor)?`, right-associative.
type ScalarFactor struct {
	Base ScalarAtom    `@@`
	Exp  *ScalarFactor `( Pow @@ )?`
}

func (f ScalarFactor) String() string {
	if f.Exp != nil {
		return f.Base.String() + " ^ " + f.Exp.String()
	}
	return f.Base.String()
}

// ScalarAtom is an aggregate, a number or duration literal, or a
// parenthesised scalar expression.
type ScalarAtom struct {
	Aggregate *Aggregate     `  @@`
	Literal   *ScalarLiteral `| @@`
	Paren     *ScalarExpr    `| "(" @@ ")"`
}

func (a ScalarAtom) String() string {
	switch {
	case a.Aggregate != nil:
		return a.Aggregate.String()
	case a.Literal != nil:
		return a.Literal.String()
	}
	return "(" + a.Paren.String() + ")"
}

// Aggregate is `count()` or `avg|min|max|sum(attribute)`.
type Aggregate struct {
	Fn   string `( @"count" "(" ")" | @("avg"|"min"|"max"|"sum") "("`
	Attr string `  @Label_name ")" )`
}

func (a Aggregate) String() string {
	return a.Fn + "(" + a.Attr + ")"
}

// ScalarLiteral is a number, with its duration unit if any.
type ScalarLiteral struct {
	Num  string `@Minus? (@Float | @Integer @Dot? @Integer?)`
	Unit string `@("ns"|"us"|"ms"|"s"|"m"|"h"|"d")?`
}

func (l ScalarLiteral) String() string {
	return l.Num + l.Unit
}

type AttrSelectorExp struct {
	Head        *AttrSelector    `(@@`
	BoolLiteral string           `| @("true"|"false")`
	ComplexHead *AttrSelectorExp `| "(" @@ ")" )`
	AndOr       string           `@(And|Or)?`
	Tail        *AttrSelectorExp `@@?`
}

func (a AttrSelectorExp) String() string {
	res := ""
	if a.Head != nil {
		res += a.Head.String()
	}
	if a.BoolLiteral != "" {
		res += a.BoolLiteral
	}
	if a.ComplexHead != nil {
		res += "(" + a.ComplexHead.String() + ")"
	}
	if a.AndOr != "" {
		res += " " + a.AndOr + " " + a.Tail.String()
	}
	return res
}

type Aggregator struct {
	Fn          string `"|" @("count"|"sum"|"min"|"max"|"avg")`
	Attr        string `"(" @Label_name? ")"`
	Cmp         string `@("="|"!="|"<"|"<="|">"|">=")`
	Num         string `@Minus? (@Float | @Integer @Dot? @Integer?)`
	Measurement string `@("ns"|"us"|"ms"|"s"|"m"|"h"|"d")?`
}

func (a Aggregator) String() string {
	return "| " + a.Fn + "(" + a.Attr + ") " + a.Cmp + " " + a.Num + a.Measurement
}

type AttrSelector struct {
	Label string `@Label_name`
	Op    string `@("="|"!="|"<"|"<="|">"|">="|"=~"|"!~")?`
	Val   Value  `@@?`
}

func (a AttrSelector) String() string {
	return a.Label + " " + a.Op + " " + a.Val.String()
}

type Value struct {
	NilVal    string        `@"nil"`
	BoolVal   string        `| @("true"|"false")`
	TimeVal   string        `| (@Float | @Integer @Dot? @Integer?) @("ns"|"us"|"ms"|"s"|"m"|"h"|"d")`
	FVal      string        `| @Minus? (@Float | @Integer @Dot? @Integer?)`
	StrVal    *QuotedString `| @@`
	UnquotVal string        `| @Label_name`
}

func (v Value) String() string {
	if v.NilVal != "" {
		return "nil"
	}
	if v.BoolVal != "" {
		return v.BoolVal
	}
	if v.StrVal != nil {
		return v.StrVal.Str
	}
	if v.FVal != "" {
		return v.FVal
	}
	if v.TimeVal != "" {
		return v.TimeVal
	}
	if v.UnquotVal != "" {
		return v.UnquotVal
	}
	return ""
}

type QuotedString struct {
	Str string `@(Quoted_string|Ticked_string) `
}

func (q QuotedString) String() string {
	return q.Str
}

func (q *QuotedString) Unquote() (string, error) {
	str := q.Str
	if q.Str[0] == '`' {
		str = str[1 : len(str)-1]
		str = strings.ReplaceAll(str, "\\`", "`")
		str = strings.ReplaceAll(str, `\`, `\\`)
		str = strings.ReplaceAll(str, `"`, `\"`)
		str = `"` + str + `"`
	}
	var res string = ""
	err := json.Unmarshal([]byte(str), &res)
	return res, err
}

// MetricsPipelineStage represents: | rate(), | count_over_time(), | compare(), etc.
type MetricsPipelineStage struct {
	Fn         string           `"|" @Label_name`
	Attr       string           `"(" @Label_name?`
	Percentile *float64         `( "," @Float )?`
	CompSel    *CompareSelector `@@?`
	Cb         string           `")"`
	By         *ByClause        `@@?`
}

func (m MetricsPipelineStage) String() string {
	res := "| " + m.Fn + "(" + m.Attr
	if m.Percentile != nil {
		res += fmt.Sprintf(", %g", *m.Percentile)
	}
	if m.CompSel != nil {
		res += m.CompSel.String()
	}
	res += ")"
	if m.By != nil {
		res += " " + m.By.String()
	}
	return res
}

// CompareSelector handles the arguments to compare():
//
//	compare({selector}, N)
//	compare({selector}, N, baselineStartNs, baselineEndNs)
type CompareSelector struct {
	AttrSelector *AttrSelectorExp `"{" @@? "}"`
	Comma        *struct{}        `","?`
	Count        int              `@Integer?`
	BaselineFrom *int64           `("," @Integer)?`
	BaselineTo   *int64           `("," @Integer)?`
}

func (c CompareSelector) String() string {
	sel := ""
	if c.AttrSelector != nil {
		sel = c.AttrSelector.String()
	}
	s := fmt.Sprintf("{%s}, %d", sel, c.Count)
	if c.BaselineFrom != nil {
		s += fmt.Sprintf(", %d", *c.BaselineFrom)
	}
	if c.BaselineTo != nil {
		s += fmt.Sprintf(", %d", *c.BaselineTo)
	}
	return s
}

// ByClause represents: by (label1, label2, ...)
type ByClause struct {
	Labels []string `"by" "(" @Label_name ( "," @Label_name )* ")"`
}

func (b ByClause) String() string {
	return "by (" + strings.Join(b.Labels, ", ") + ")"
}

// WithClause represents: with(key=value, ...)
type WithClause struct {
	Hints []WithHint `"with" "(" @@ ( "," @@ )* ")"`
}

func (w WithClause) String() string {
	parts := make([]string, len(w.Hints))
	for i, h := range w.Hints {
		parts[i] = h.String()
	}
	return "with(" + strings.Join(parts, ", ") + ")"
}

// WithHint represents a single key=value hint like sample=true or sample=0.1
type WithHint struct {
	Key   string `@Label_name "="`
	Value string `@("true"|"false"|Float|Integer|Label_name)`
}

func (h WithHint) String() string {
	return h.Key + "=" + h.Value
}

// SecondPipelineStage represents: | topk(N), | bottomk(N), or | select(attr1, attr2, ...)
type SecondPipelineStage struct {
	Fn        string      `"|" @("topk"|"bottomk"|"select")`
	Count     int         `"(" ( @Integer`
	Labels    []string    `| @Label_name ( "," @Label_name )* ) ")"`
	WithHints *WithClause `@@?`
}

func (s SecondPipelineStage) String() string {
	if s.Fn == "select" {
		return fmt.Sprintf("| select(%s)", strings.Join(s.Labels, ", "))
	}
	return fmt.Sprintf("| %s(%d)", s.Fn, s.Count)
}
