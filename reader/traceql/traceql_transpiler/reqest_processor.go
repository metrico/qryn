package traceql_transpiler

import (
	"fmt"
	"strconv"
	"time"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	"github.com/metrico/qryn/v5/reader/model"
	"github.com/metrico/qryn/v5/reader/traceql/traceql_transpiler/clickhouse_transpiler"
	"github.com/metrico/qryn/v5/reader/utils/logger"
	"github.com/metrico/qryn/v5/reader/utils/sql_select"
)

type TraceQLRequestProcessor struct {
	sqlPlanner shared.SQLRequestPlanner
}

func (t TraceQLRequestProcessor) Process(ctx *shared.PlannerContext) (chan []model.TraceInfo, error) {
	sqlReq, err := t.sqlPlanner.Process(ctx)
	if err != nil {
		return nil, err
	}

	strReq, err := sqlReq.String(&sql_select.Ctx{
		Params: map[string]sql_select.SQLObject{},
		Result: map[string]sql_select.SQLObject{},
	})
	if err != nil {
		return nil, err
	}

	traces, err := t.read(ctx, strReq)
	if err != nil {
		return nil, clickhouse_transpiler.UnresolvedAncestryError(ctx, err)
	}
	res := make(chan []model.TraceInfo)
	go func() {
		defer close(res)
		for _, trace := range traces {
			res <- []model.TraceInfo{trace}
		}
	}()
	return res, nil
}

// read runs the search query and reads every trace, so that an error raised
// while ClickHouse streams the result fails the request.
func (t TraceQLRequestProcessor) read(ctx *shared.PlannerContext, strReq string) ([]model.TraceInfo, error) {
	rows, err := ctx.CHDb.QueryCtx(ctx.Ctx, strReq)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var res []model.TraceInfo
	for rows.Next() {
		var (
			traceId           string
			spanIds           []string
			durationsNs       []int64
			timestampsNs      []int64
			startTimeUnixNano int64
			traceDurationMs   int64
			rootServiceName   string
			rootTraceName     string
			matched           int64
			attrNames         []string
			attrTypes         []string
			attrValues        []float64
		)
		err = rows.Scan(&traceId, &spanIds, &durationsNs, &timestampsNs,
			&startTimeUnixNano, &traceDurationMs, &rootServiceName, &rootTraceName, &matched,
			&attrNames, &attrTypes, &attrValues)
		if err != nil {
			logger.Error("ERROR[TRP#1]: ", err)
			return nil, err
		}
		for i := range durationsNs {
			if durationsNs[i] == timestampsNs[i] {
				durationsNs[i] = -1
			}
		}
		trace := model.TraceInfo{
			TraceID:           traceId,
			RootServiceName:   rootServiceName,
			RootTraceName:     rootTraceName,
			StartTimeUnixNano: fmt.Sprintf("%d", startTimeUnixNano),
			DurationMs:        traceDurationMs,
			SpanSet: model.SpanSet{
				Spans: make([]model.SpanInfo, len(spanIds)),
			},
		}
		for i, spanId := range spanIds {
			trace.SpanSet.Spans[i].SpanID = spanId
			trace.SpanSet.Spans[i].DurationNanos = fmt.Sprintf("%d", durationsNs[i])
			if durationsNs[i] == -1 {
				trace.SpanSet.Spans[i].DurationNanos = "n/a"
			}
			trace.SpanSet.Spans[i].StartTimeUnixNano = fmt.Sprintf("%d", timestampsNs[i])
			trace.SpanSet.Spans[i].Attributes = make([]model.SpanAttr, 0)
		}
		trace.SpanSet.Matched = int(matched)
		trace.SpanSet.Attributes = spanSetAttrs(attrNames, attrTypes, attrValues)
		trace.SpanSets = []model.SpanSet{trace.SpanSet}
		sortSpans(trace.SpanSet.Spans)
		res = append(res, trace)
	}
	return res, rows.Err()
}

// spanSetAttrs builds spanset attributes from their names, types and values.
func spanSetAttrs(names, types []string, values []float64) []model.SpanSetAttr {
	if len(names) == 0 {
		return nil
	}
	res := make([]model.SpanSetAttr, len(names))
	for i, name := range names {
		res[i].Key = name
		switch types[i] {
		case clickhouse_transpiler.AttrInt:
			v := strconv.FormatInt(int64(values[i]), 10)
			res[i].Value.IntValue = &v
		case clickhouse_transpiler.AttrDuration:
			v := time.Duration(int64(values[i])).String()
			res[i].Value.StringValue = &v
		default:
			v := model.Double(values[i])
			res[i].Value.DoubleValue = &v
		}
	}
	return res
}
