package transpiler

import (
	jsoniter "github.com/json-iterator/go"
	"github.com/metrico/qryn/reader/logql/logql_transpiler_v2/shared"
	"github.com/metrico/qryn/reader/prof/parser"
	v1 "github.com/metrico/qryn/reader/prof/types/v1"
	sql "github.com/metrico/qryn/reader/utils/sql_select"
)

type SelectSeriesPlanner struct {
	GetLabelsPlanner shared.SQLRequestPlanner
	Selectors        []parser.Selector
	SampleType       string
	SampleUnit       string
	Aggregation      v1.TimeSeriesAggregationType
	Step             int64
}

func (s *SelectSeriesPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	labels, err := s.GetLabelsPlanner.Process(ctx)
	if err != nil {
		return nil, err
	}

	matchers, err := (&StreamSelectorPlanner{Selectors: s.Selectors}).getMatchers()
	if err != nil {
		return nil, err
	}

	//sampleTypeUnit := fmt.Sprintf("%s:%s", s.SampleType, s.SampleUnit)
	json := jsoniter.ConfigFastest
	stream := json.BorrowStream(nil)
	defer json.ReturnStream(stream)

	stream.WriteString(s.SampleType)
	stream.WriteRaw(":")
	stream.WriteString(s.SampleUnit)

	sampleTypeUnit := string(stream.Buffer())
	sampleTypeUnitCond := sql.Eq(sql.NewRawObject("x.1"), sql.NewStringVal(sampleTypeUnit))

	valueCol := sql.NewCol(sql.NewCustomCol(func(ctx *sql.Ctx, options ...int) (string, error) {
		strSampleTypeUnit, err := sampleTypeUnitCond.String(ctx, options...)
		if err != nil {
			return "", err
		}
		json := jsoniter.ConfigFastest
		stream := json.BorrowStream(nil)
		defer json.ReturnStream(stream)

		stream.WriteRaw("sum(toFloat64(arrayFirst(x -> ")
		stream.WriteString(strSampleTypeUnit)
		stream.WriteRaw(", p.values_agg).2))")

		if stream.Error != nil {
			return "", stream.Error
		}

		// Copy the buffer to avoid data being overwritten when the stream is reused
		result := make([]byte, len(stream.Buffer()))
		copy(result, stream.Buffer())

		return string(result), nil
		//return fmt.Sprintf("sum(toFloat64(arrayFirst(x -> %s, p.values_agg).2))", strSampleTypeUnit), nil
	}), "value")
	if s.Aggregation == v1.TimeSeriesAggregationType_TIME_SERIES_AGGREGATION_TYPE_AVERAGE {
		valueCol = sql.NewCol(sql.NewCustomCol(func(ctx *sql.Ctx, options ...int) (string, error) {
			strSampleTypeUnit, err := sampleTypeUnitCond.String(ctx, options...)
			if err != nil {
				return "", err
			}
			//return fmt.Sprintf(
			//	"sum(toFloat64(arrayFirst(x -> %s, p.values_agg).2)) / "+
			//		"sum(toFloat64(arrayFirst(x -> x.1 == %s).3))",
			//	strSampleTypeUnit, strSampleTypeUnit), nil
			json := jsoniter.ConfigFastest
			stream := json.BorrowStream(nil)
			defer json.ReturnStream(stream)

			stream.WriteRaw("sum(toFloat64(arrayFirst(x -> ")
			stream.WriteString(strSampleTypeUnit)
			stream.WriteRaw(", p.values_agg).2)) / sum(toFloat64(arrayFirst(x -> x.1 == ")
			stream.WriteString(strSampleTypeUnit)
			stream.WriteRaw(").3))")
			result := string(stream.Buffer())
			return result, nil
		}), "value")
	}

	withLabels := sql.NewWith(labels, "labels")
	var withFP *sql.With
	for _, w := range labels.GetWith() {
		if w.GetAlias() == "fp" {
			withFP = w
			break
		}
	}
	stream.Reset(nil)
	stream.WriteRaw("intDiv(p.timestamp_ns, 1000000000 * ")
	stream.WriteInt64(s.Step)
	stream.WriteRaw(") * ")
	stream.WriteInt64(s.Step)
	stream.WriteRaw(" * 1000")
	timestampExpr := string(stream.Buffer())
	if stream.Error != nil {
		return nil, stream.Error
	}
	main := sql.NewSelect().
		With(withLabels).
		Select(
			sql.NewSimpleCol(timestampExpr,
				//	fmt.Sprintf("intDiv(p.timestamp_ns, 1000000000 * %d) * %d * 1000", s.Step, s.Step),
				"timestamp_ms"),
			//sql.NewSimpleCol(timestampExpr, "timestamp_ms"),
			sql.NewSimpleCol("labels.new_fingerprint", "fingerprint"),
			sql.NewSimpleCol("min(labels.tags)", "labels"),
			valueCol).
		From(sql.NewSimpleCol(ctx.ProfilesDistTable, "p")).
		Join(sql.NewJoin("any left", sql.NewWithRef(withLabels),
			sql.Eq(sql.NewRawObject("p.fingerprint"), sql.NewRawObject("labels.fingerprint")))).
		AndWhere(
			sql.NewIn(sql.NewRawObject("p.fingerprint"), sql.NewWithRef(withFP)),
			sql.Ge(sql.NewRawObject("p.timestamp_ns"), sql.NewIntVal(ctx.From.UnixNano())),
			sql.Le(sql.NewRawObject("p.timestamp_ns"), sql.NewIntVal(ctx.To.UnixNano()))).
		GroupBy(sql.NewRawObject("timestamp_ms"), sql.NewRawObject("fingerprint")).
		OrderBy(sql.NewOrderBy(sql.NewRawObject("fingerprint"), sql.ORDER_BY_DIRECTION_ASC),
			sql.NewOrderBy(sql.NewRawObject("timestamp_ms"), sql.ORDER_BY_DIRECTION_ASC))
	if len(matchers.globalMatchers) > 0 {
		main.AndWhere(matchers.globalMatchers...)
	}
	return main, nil
}
