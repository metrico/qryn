package main

import (
	"context"
	"fmt"
	gcContext "github.com/metrico/micro-gc/context"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"strconv"
	"strings"
	"time"
	"unsafe"
	promql2 "wasm_parts/promql"
	shared2 "wasm_parts/promql/shared"
	sql "wasm_parts/sql_select"
	parser2 "wasm_parts/traceql/parser"
	traceql_transpiler "wasm_parts/traceql/transpiler"
	"wasm_parts/types"
)

var maxSamples = 5000000

type ctx struct {
	onDataLoad func(c *ctx)
	request    []byte
	response   []byte
}

var data = map[uint32]*ctx{}

//export createCtx
func createCtx(id uint32) {
	ctxId := gcContext.GetContextID()
	gcContext.SetContext(id)
	c := &ctx{}
	gcContext.SetContext(ctxId)
	data[id] = c
}

//export alloc
func alloc(id uint32, size int) *byte {
	ctxId := gcContext.GetContextID()
	gcContext.SetContext(id)
	data[id].request = make([]byte, size)
	gcContext.SetContext(ctxId)
	return &data[id].request[0]
}

//export dealloc
func dealloc(id uint32) {
	delete(data, id)
	gcContext.ReleaseContext(id)
}

//export getCtxRequest
func getCtxRequest(id uint32) *byte {
	return &data[id].request[0]
}

//export getCtxRequestLen
func getCtxRequestLen(id uint32) uint32 {
	return uint32(len(data[id].request))
}

//export getCtxResponse
func getCtxResponse(id uint32) *byte {
	return &data[id].response[0]
}

//export getCtxResponseLen
func getCtxResponseLen(id uint32) uint32 {
	return uint32(len(data[id].response))
}

//export transpileTraceQL
func transpileTraceQL(id uint32) int {
	ctxId := gcContext.GetContextID()
	gcContext.SetContext(id)
	defer gcContext.SetContext(ctxId)

	request := types.TraceQLRequest{}
	err := request.UnmarshalJSON(data[id].request)
	if err != nil {
		data[id].response = []byte(err.Error())
		return 1
	}

	script, err := parser2.Parse(request.Request)
	if err != nil {
		data[id].response = []byte(err.Error())
		return 1
	}

	planner, err := traceql_transpiler.Plan(script)
	if err != nil {
		data[id].response = []byte(err.Error())
		return 1
	}
	request.Ctx.Ctx = context.Background()
	request.Ctx.CancelCtx = func() {}
	request.Ctx.CHSqlCtx = &sql.Ctx{
		Params: make(map[string]sql.SQLObject),
		Result: make(map[string]sql.SQLObject),
	}
	request.Ctx.From = time.Unix(int64(request.Ctx.FromS), 0)
	request.Ctx.To = time.Unix(int64(request.Ctx.ToS), 0)
	sel, err := planner.Process(&request.Ctx)
	if err != nil {
		data[id].response = []byte(err.Error())
		return 1
	}
	var options []int
	if request.Ctx.IsCluster {
		options = append(options, sql.STRING_OPT_INLINE_WITH)
	}
	str, err := sel.String(request.Ctx.CHSqlCtx, options...)
	print(str)
	print("\n")
	if err != nil {
		data[id].response = []byte(err.Error())
		return 1
	}
	data[id].response = []byte(str)
	return 0
}

var eng *promql.Engine = promql.NewEngine(promql.EngineOpts{
	Logger:                   TestLogger{},
	MaxSamples:               maxSamples,
	Timeout:                  time.Second * 30,
	ActiveQueryTracker:       nil,
	LookbackDelta:            0,
	NoStepSubqueryIntervalFn: nil,
	EnableAtModifier:         false,
	EnableNegativeOffset:     false,
})
var engC = func() *promql.Engine {
	return promql.NewEngine(promql.EngineOpts{
		Logger:                   TestLogger{},
		MaxSamples:               maxSamples,
		Timeout:                  time.Second * 30,
		ActiveQueryTracker:       nil,
		LookbackDelta:            0,
		NoStepSubqueryIntervalFn: nil,
		EnableAtModifier:         false,
		EnableNegativeOffset:     false,
	})
}()

func getEng() *promql.Engine {
	return eng
}

//export setMaxSamples
func setMaxSamples(maxSpl int) {
	maxSamples = maxSpl
}

//export stats
func stats() {
	fmt.Printf("Allocated data: %d\n", len(data))
}

//export pqlRangeQuery
func pqlRangeQuery(id uint32, fromMS float64, toMS float64, stepMS float64, optimizable uint32) uint32 {
	ctxId := gcContext.GetContextID()
	gcContext.SetContext(id)
	defer gcContext.SetContext(ctxId)

	return pql(id, data[id], optimizable != 0, int64(fromMS), int64(toMS), int64(stepMS), func() (promql.Query, error) {
		queriable := &TestQueryable{id: id, stepMs: int64(stepMS)}
		return getEng().NewRangeQuery(
			queriable,
			nil,
			string(data[id].request),
			time.Unix(0, int64(fromMS)*1000000),
			time.Unix(0, int64(toMS)*1000000),
			time.Millisecond*time.Duration(stepMS))
	})

}

//export pqlInstantQuery
func pqlInstantQuery(id uint32, timeMS float64, optimizable uint32) uint32 {
	ctxId := gcContext.GetContextID()
	gcContext.SetContext(id)
	defer gcContext.SetContext(ctxId)

	return pql(id, data[id], optimizable != 0, int64(timeMS-300000), int64(timeMS), 15000,
		func() (promql.Query, error) {
			queriable := &TestQueryable{id: id, stepMs: 15000}
			return getEng().NewInstantQuery(
				queriable,
				nil,
				string(data[id].request),
				time.Unix(0, int64(timeMS)*1000000))
		})
}

//export pqlSeries
func pqlSeries(id uint32) uint32 {
	ctxId := gcContext.GetContextID()
	gcContext.SetContext(id)
	defer gcContext.SetContext(ctxId)

	queriable := &TestQueryable{id: id, stepMs: 15000}
	query, err := getEng().NewRangeQuery(
		queriable,
		nil,
		string(data[id].request),
		time.Unix(0, 1),
		time.Unix(0, 2),
		time.Second)
	if err != nil {
		data[id].response = wrapError(err)
		return 1
	}
	data[id].response = []byte(getmatchersJSON(query))
	return 0
}

func getmatchersJSON(q promql.Query) string {
	var matchersJson = strings.Builder{}
	var walk func(node parser.Node, i func(node parser.Node))
	walk = func(node parser.Node, i func(node parser.Node)) {
		i(node)
		for _, n := range parser.Children(node) {
			walk(n, i)
		}
	}
	i := 0
	matchersJson.WriteString("[")
	walk(q.Statement(), func(node parser.Node) {
		switch n := node.(type) {
		case *parser.VectorSelector:
			if i != 0 {
				matchersJson.WriteString(",")
			}
			matchersJson.WriteString(matchers2Str(n.LabelMatchers))
			i++
		}
	})
	matchersJson.WriteString("]")
	return matchersJson.String()
}

func wrapError(err error) []byte {
	return []byte(wrapErrorStr(err))
}

func wrapErrorStr(err error) string {
	//return fmt.Sprintf(`{"status":"error", "error":%s}`, strconv.Quote(err.Error()))
	return err.Error()
}

func pql(id uint32, c *ctx, optimizable bool,
	fromMs int64, toMs int64, stepMs int64,
	query func() (promql.Query, error)) uint32 {
	rq, err := query()

	if err != nil {
		c.response = wrapError(err)
		return 1
	}

	var walk func(node parser.Node, i func(node parser.Node))
	walk = func(node parser.Node, i func(node parser.Node)) {
		i(node)
		for _, n := range parser.Children(node) {
			walk(n, i)
		}
	}

	subsels := strings.Builder{}
	subsels.WriteString("{")
	if optimizable {
		var (
			subselsMap map[string]string
			err        error
		)
		subselsMap, rq, err = optimizeQuery(rq, fromMs, toMs, stepMs)
		if err != nil {
			c.response = wrapError(err)
			return 1
		}
		i := 0
		for k, v := range subselsMap {
			if i != 0 {
				subsels.WriteString(",")
			}
			subsels.WriteString(fmt.Sprintf(`"%s":"%s"`, strconv.Quote(k), strconv.Quote(v)))
			i++
		}
	}
	subsels.WriteString("}")

	matchersJSON := getmatchersJSON(rq)

	c.response = []byte(fmt.Sprintf(`{"subqueries": %s, "matchers": %s}`, subsels.String(), matchersJSON))
	c.onDataLoad = func(c *ctx) {
		ctxId := gcContext.GetContextID()
		gcContext.SetContext(id)
		defer gcContext.SetContext(ctxId)

		res := rq.Exec(context.Background())
		c.response = []byte(writeResponse(res))
		return
	}
	return 0
}

func optimizeQuery(q promql.Query, fromMs int64, toMs int64, stepMs int64) (map[string]string, promql.Query, error) {
	appliableNodes := findAppliableNodes(q.Statement(), nil)
	var err error
	subsels := make(map[string]string)
	for _, m := range appliableNodes {
		fmt.Println(m)
		opt := m.optimizer
		opt = &promql2.FinalizerOptimizer{
			SubOptimizer: opt,
		}
		opt, err = promql2.PlanOptimize(m.node, opt)
		if err != nil {
			return nil, nil, err
		}
		planner, err := opt.Optimize(m.node)
		if err != nil {
			return nil, nil, err
		}
		fakeMetric := fmt.Sprintf("fake_metric_%d", time.Now().UnixNano())
		swapChild(m.parent, m.node, &parser.VectorSelector{
			Name:           fakeMetric,
			OriginalOffset: 0,
			Offset:         0,
			Timestamp:      nil,
			StartOrEnd:     0,
			LabelMatchers: []*labels.Matcher{
				{
					Type:  labels.MatchEqual,
					Name:  "__name__",
					Value: fakeMetric,
				},
			},
			UnexpandedSeriesSet: nil,
			Series:              nil,
			PosRange:            parser.PositionRange{},
		})
		sel, err := planner.Process(&shared2.PlannerContext{
			IsCluster:           false,
			From:                time.Unix(0, fromMs*1000000),
			To:                  time.Unix(0, toMs*1000000),
			Step:                time.Millisecond * time.Duration(stepMs),
			TimeSeriesTable:     "time_series",
			TimeSeriesDistTable: "time_series_dist",
			TimeSeriesGinTable:  "time_series_gin",
			MetricsTable:        "metrics_15s",
			MetricsDistTable:    "metrics_15s_dist",
		})
		if err != nil {
			return nil, nil, err
		}
		strSel, err := sel.String(&sql.Ctx{
			Params: map[string]sql.SQLObject{},
			Result: map[string]sql.SQLObject{},
		})
		if err != nil {
			return nil, nil, err
		}
		subsels[fakeMetric] = strSel
	}
	return subsels, q, nil
}

//export onDataLoad
func onDataLoad(idx uint32) {
	data[idx].onDataLoad(data[idx])
}

func writeResponse(res *promql.Result) string {
	if res.Err != nil {
		return wrapErrorStr(res.Err)
	}
	switch res.Value.Type() {
	case parser.ValueTypeMatrix:
		m, err := res.Matrix()
		if err != nil {
			return wrapErrorStr(err)
		}
		return writeMatrix(m)
	case parser.ValueTypeVector:
		v, err := res.Vector()
		if err != nil {
			return wrapErrorStr(err)
		}
		return writeVector(v)
	}
	return wrapErrorStr(fmt.Errorf("result type not supported"))
}

func writeMatrix(m promql.Matrix) string {
	jsonBuilder := strings.Builder{}
	jsonBuilder.WriteString(`{"status": "success", "data": {"resultType":"matrix","result":[`)
	for i, s := range m {
		if i != 0 {
			jsonBuilder.WriteString(",")
		}
		jsonBuilder.WriteString(`{"metric": {`)
		for j, l := range s.Metric {
			if j != 0 {
				jsonBuilder.WriteString(",")
			}
			jsonBuilder.WriteString(fmt.Sprintf("%s:%s", strconv.Quote(l.Name), strconv.Quote(l.Value)))
		}
		jsonBuilder.WriteString(`}, "values": [`)
		for j, v := range s.Points {
			if j != 0 {
				jsonBuilder.WriteString(",")
			}
			jsonBuilder.WriteString(fmt.Sprintf("[%d,\"%f\"]", v.T/1000, v.V))
		}
		jsonBuilder.WriteString(`]}`)
	}
	jsonBuilder.WriteString(`]}}`)
	return jsonBuilder.String()
}

func writeVector(v promql.Vector) string {
	jsonBuilder := strings.Builder{}
	jsonBuilder.WriteString(`{"status": "success", "data": {"resultType":"vector","result":[`)
	for i, s := range v {
		if i != 0 {
			jsonBuilder.WriteString(",")
		}
		jsonBuilder.WriteString(`{"metric": {`)
		for j, l := range s.Metric {
			if j != 0 {
				jsonBuilder.WriteString(",")
			}
			jsonBuilder.WriteString(fmt.Sprintf("%s:%s", strconv.Quote(l.Name), strconv.Quote(l.Value)))
		}
		jsonBuilder.WriteString(fmt.Sprintf(`}, "value": [%d,"%f"]}`, s.T/1000, s.V))
	}
	jsonBuilder.WriteString(`]}}`)
	return jsonBuilder.String()
}

func main() {
	queriable := &TestQueryable{id: 0, stepMs: 15000}
	rq, err := getEng().NewRangeQuery(
		queriable,
		nil,
		"histogram_quantile(0.5, sum by (container) (rate(network_usage{container=~\"awesome\"}[5m])))",
		time.Now().Add(time.Hour*-24),
		time.Now(),
		time.Millisecond*time.Duration(15000))
	if err != nil {
		panic(err)
	}
	matchers := findAppliableNodes(rq.Statement(), nil)
	for _, m := range matchers {
		fmt.Println(m)
		opt := m.optimizer
		opt = &promql2.FinalizerOptimizer{
			SubOptimizer: opt,
		}
		opt, err = promql2.PlanOptimize(m.node, opt)
		if err != nil {
			panic(err)
		}
		planner, err := opt.Optimize(m.node)
		if err != nil {
			panic(err)
		}
		fakeMetric := fmt.Sprintf("fake_metric_%d", time.Now().UnixNano())
		fmt.Println(rq.Statement())
		swapChild(m.parent, m.node, &parser.VectorSelector{
			Name:           fakeMetric,
			OriginalOffset: 0,
			Offset:         0,
			Timestamp:      nil,
			StartOrEnd:     0,
			LabelMatchers: []*labels.Matcher{
				{
					Type:  labels.MatchEqual,
					Name:  "__name__",
					Value: fakeMetric,
				},
			},
			UnexpandedSeriesSet: nil,
			Series:              nil,
			PosRange:            parser.PositionRange{},
		})
		fmt.Println(rq.Statement())
		sel, err := planner.Process(&shared2.PlannerContext{
			IsCluster:           false,
			From:                time.Now().Add(time.Hour * -24),
			To:                  time.Now(),
			Step:                time.Millisecond * time.Duration(15000),
			TimeSeriesTable:     "time_series",
			TimeSeriesDistTable: "time_series_dist",
			TimeSeriesGinTable:  "time_series_gin",
			MetricsTable:        "metrics_15s",
			MetricsDistTable:    "metrics_15s_dist",
		})
		if err != nil {
			panic(err)
		}
		strSel, err := sel.String(&sql.Ctx{
			Params: map[string]sql.SQLObject{},
			Result: map[string]sql.SQLObject{},
		})
		if err != nil {
			panic(err)
		}
		println(strSel)
	}

}

func getOptimizer(n parser.Node) promql2.IOptimizer {
	for _, f := range promql2.Optimizers {
		opt := f()
		if opt.IsAppliable(n) {
			return opt
		}
	}
	return nil
}

func isRate(node parser.Node) (bool, bool) {
	opt := getOptimizer(node)
	if opt == nil {
		return false, true
	}
	return true, false
}

type MatchNode struct {
	node      parser.Node
	parent    parser.Node
	optimizer promql2.IOptimizer
}

func findAppliableNodes(root parser.Node, parent parser.Node) []MatchNode {
	var res []MatchNode
	optimizer := getOptimizer(root)
	if optimizer != nil {
		res = append(res, MatchNode{
			node:      root,
			parent:    parent,
			optimizer: optimizer,
		})
		return res
	}
	for _, n := range parser.Children(root) {
		res = append(res, findAppliableNodes(n, root)...)
	}
	return res
}

func swapChild(node parser.Node, child parser.Node, newChild parser.Expr) {
	// For some reasons these switches have significantly better performance than interfaces
	switch n := node.(type) {
	case *parser.EvalStmt:
		n.Expr = newChild
	case parser.Expressions:
		for i, e := range n {
			if e.String() == child.String() {
				n[i] = newChild
			}
		}
	case *parser.AggregateExpr:
		if n.Expr == nil && n.Param == nil {
			return
		} else if n.Expr == nil {
			n.Param = newChild
		} else if n.Param == nil {
			n.Expr = newChild
		} else {
			if n.Expr.String() == child.String() {
				n.Expr = newChild
			} else {
				n.Param = newChild
			}
		}
	case *parser.BinaryExpr:
		if n.LHS.String() == child.String() {
			n.LHS = newChild
		} else if n.RHS.String() == child.String() {
			n.RHS = newChild
		}
	case *parser.Call:
		for i, e := range n.Args {
			if e.String() == child.String() {
				n.Args[i] = newChild
			}
		}
	case *parser.SubqueryExpr:
		n.Expr = newChild
	case *parser.ParenExpr:
		n.Expr = newChild
	case *parser.UnaryExpr:
		n.Expr = newChild
	case *parser.MatrixSelector:
		n.VectorSelector = newChild
	case *parser.StepInvariantExpr:
		n.Expr = newChild
	}
}

func getChildren(e parser.Node) []parser.Node {
	return parser.Children(e)
}

type TestLogger struct{}

func (t TestLogger) Log(keyvals ...interface{}) error {
	fmt.Print(keyvals...)
	fmt.Print("\n")
	return nil
}

type TestQueryable struct {
	id     uint32
	stepMs int64
}

func (t TestQueryable) Querier(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
	sets := make(map[string][]byte)
	r := BinaryReader{buffer: data[t.id].request}
	for r.i < uint32(len(data[t.id].request)) {
		sets[r.ReadString()] = r.ReadByteArray()
	}
	return &TestQuerier{sets: sets, stepMs: t.stepMs}, nil
}

type TestQuerier struct {
	sets   map[string][]byte
	stepMs int64
}

func (t TestQuerier) LabelValues(name string, matchers ...*labels.Matcher) ([]string, storage.Warnings, error) {
	return nil, nil, nil
}

func (t TestQuerier) LabelNames(matchers ...*labels.Matcher) ([]string, storage.Warnings, error) {
	return nil, nil, nil
}

func (t TestQuerier) Close() error {
	return nil
}

func (t TestQuerier) Select(sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	strMatchers := matchers2Str(matchers)
	return &TestSeriesSet{
		data:   t.sets[strMatchers],
		reader: BinaryReader{buffer: t.sets[strMatchers]},
		stepMs: t.stepMs,
	}
}

type TestSeriesSet struct {
	data   []byte
	reader BinaryReader
	stepMs int64
}

func (t *TestSeriesSet) Next() bool {
	return t.reader.i < uint32(len(t.data))
}

func (t *TestSeriesSet) At() storage.Series {
	res := &TestSeries{
		i:      0,
		stepMs: t.stepMs,
	}
	res.labels = t.reader.ReadLabelsTuple()
	res.data = t.reader.ReadPointsArrayRaw()
	res.reset()
	return res
}

func (t *TestSeriesSet) Err() error {
	return nil
}

func (t *TestSeriesSet) Warnings() storage.Warnings {
	return nil
}

type TestSeries struct {
	data   []byte
	stepMs int64

	labels labels.Labels
	tsMs   int64
	val    float64
	i      int

	state int
}

func (t *TestSeries) reset() {
	if len(t.data) == 0 {
		return
	}
	t.tsMs = *(*int64)(unsafe.Pointer(&t.data[0]))
	t.val = *(*float64)(unsafe.Pointer(&t.data[t.i*16+8]))
}

func (t *TestSeries) Next() bool {
	if t.i*16 >= len(t.data) {
		return false
	}
	ts := *(*int64)(unsafe.Pointer(&t.data[t.i*16]))
	if t.state == 1 {
		t.tsMs += t.stepMs
		if t.tsMs >= ts {
			t.state = 0
		}
	}
	if t.state == 0 {
		t.tsMs = ts
		t.val = *(*float64)(unsafe.Pointer(&t.data[t.i*16+8]))
		t.i++
		t.state = 1
	}
	return true
}

func (t *TestSeries) Seek(tmMS int64) bool {
	for t.i = 0; t.i*16 < len(t.data); t.i++ {
		ms := *(*int64)(unsafe.Pointer(&t.data[t.i*16]))
		if ms == tmMS {
			t.tsMs = ms
			t.val = *(*float64)(unsafe.Pointer(&t.data[t.i*16+8]))
			t.i++
			return true
		}
		if ms > tmMS {
			t.i--
			if t.i < 0 {
				t.i = 0
			}
			t.tsMs = ms
			t.val = *(*float64)(unsafe.Pointer(&t.data[t.i*16+8]))
			t.i++
			return true
		}
	}
	return false
}

func (t *TestSeries) At() (int64, float64) {
	return t.tsMs, t.val
}

func (t *TestSeries) Err() error {
	return nil
}

func (t *TestSeries) Labels() labels.Labels {
	return t.labels
}

func (t *TestSeries) Iterator() chunkenc.Iterator {
	return t
}

type BinaryReader struct {
	buffer []byte
	i      uint32
}

func (b *BinaryReader) ReadULeb32() uint32 {
	var res uint32
	i := uint32(0)
	for ; b.buffer[b.i+i]&0x80 == 0x80; i++ {
		res |= uint32(b.buffer[b.i+i]&0x7f) << (i * 7)
	}
	res |= uint32(b.buffer[b.i+i]&0x7f) << (i * 7)
	b.i += i + 1
	return res
}

func (b *BinaryReader) ReadLabelsTuple() labels.Labels {
	ln := b.ReadULeb32()
	res := make(labels.Labels, ln)
	for i := uint32(0); i < ln; i++ {
		ln := b.ReadULeb32()
		res[i].Name = string(b.buffer[b.i : b.i+ln])
		b.i += ln
		ln = b.ReadULeb32()
		res[i].Value = string(b.buffer[b.i : b.i+ln])
		b.i += ln
	}
	return res
}

func (b *BinaryReader) ReadPointsArrayRaw() []byte {
	ln := b.ReadULeb32()
	res := b.buffer[b.i : b.i+(ln*16)]
	b.i += ln * 16
	return res
}

func (b *BinaryReader) ReadString() string {
	ln := b.ReadULeb32()
	res := string(b.buffer[b.i : b.i+ln])
	b.i += ln
	return res
}

func (b *BinaryReader) ReadByteArray() []byte {
	ln := b.ReadULeb32()
	res := b.buffer[b.i : b.i+ln]
	b.i += ln
	return res
}

func matchers2Str(labelMatchers []*labels.Matcher) string {
	matchersJson := strings.Builder{}
	matchersJson.WriteString("[")
	for j, m := range labelMatchers {
		if j != 0 {
			matchersJson.WriteString(",")
		}
		matchersJson.WriteString(fmt.Sprintf(`[%s,"%s",%s]`,
			strconv.Quote(m.Name),
			m.Type,
			strconv.Quote(m.Value)))
	}
	matchersJson.WriteString("]")
	return matchersJson.String()
}

type pqlRequest struct {
	optimizable bool
	body        string
}

func (p *pqlRequest) Read(body []byte) {
	r := BinaryReader{buffer: body}
	p.optimizable = r.ReadULeb32() != 0
	p.body = r.ReadString()
	if !p.optimizable {
		return
	}
}
