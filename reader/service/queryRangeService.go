package service

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/metrico/qryn/reader/logql/logql_transpiler_v2"
	"github.com/metrico/qryn/reader/logql/logql_transpiler_v2/shared"
	"github.com/metrico/qryn/reader/model"
	"github.com/metrico/qryn/reader/plugins"
	"github.com/metrico/qryn/reader/utils/dbVersion"
	"github.com/metrico/qryn/reader/utils/tables"
	"io"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/metrico/qryn/reader/utils/logger"
	sql "github.com/metrico/qryn/reader/utils/sql_select"
)

type QueryRangeService struct {
	model.ServiceData
	plugin plugins.QueryRangeServicePlugin
}

func NewQueryRangeService(data *model.ServiceData) *QueryRangeService {
	res := &QueryRangeService{
		ServiceData: *data,
	}
	p := plugins.GetQueryRangeServicePlugin()
	if p != nil {
		(*p).SetServiceData(data)
		res.plugin = *p
	}
	return res
}

func hashLabels(labels [][]interface{}) string {
	_labels := make([]string, len(labels))
	for i, l := range labels {
		val, _ := json.Marshal(l[1].(string))
		_labels[i] = fmt.Sprintf("\"%s\":%s", l[0].(string), val)
	}
	return fmt.Sprintf("{%s}", strings.Join(_labels, ","))
}

func hashLabelsMap(labels map[string]string) string {
	_labels := make([]string, len(labels))
	i := 0
	for k, v := range labels {
		val, _ := json.Marshal(v)
		_labels[i] = fmt.Sprintf("\"%s\":%s", k, val)
		i++
	}
	sort.Strings(_labels)
	return fmt.Sprintf("{%s}", strings.Join(_labels, ","))
}

func onErr(err error, res chan model.QueryRangeOutput) {
	logger.Error(err)
	res <- model.QueryRangeOutput{
		Str: "]}}",
		Err: err,
	}
}

func (q *QueryRangeService) exportStreamsValue(out chan []shared.LogEntry,
	res chan model.QueryRangeOutput) {
	defer close(res)

	res <- model.QueryRangeOutput{Str: `{"status": "success","data": {"resultType": "streams", "result": [`}

	var lastFp uint64
	i := 0
	j := 0

	for entries := range out {
		for _, e := range entries {
			if e.Err == io.EOF {
				continue
			}
			if e.Err != nil {
				onErr(e.Err, res)
				return
			}
			if lastFp != e.Fingerprint {
				if i > 0 {
					res <- model.QueryRangeOutput{Str: "]},"}
				}
				lastFp = e.Fingerprint
				i = 1
				j = 0
				stream, _ := json.Marshal(e.Labels)
				res <- model.QueryRangeOutput{Str: fmt.Sprintf(`{%s:%s, %s: [`,
					strconv.Quote("stream"), string(stream), strconv.Quote("values"))}
			}
			if j > 0 {
				res <- model.QueryRangeOutput{Str: ","}
			}
			j = 1
			msg, err := json.Marshal(e.Message)
			if err != nil {
				msg = []byte("error string")
			}
			res <- model.QueryRangeOutput{
				Str: fmt.Sprintf(`["%d", %s]`, e.TimestampNS, msg),
			}
		}
	}

	if i > 0 {
		res <- model.QueryRangeOutput{Str: "]}"}
	}
	res <- model.QueryRangeOutput{Str: "]}}"}
}

func (q *QueryRangeService) prepareOutput(ctx context.Context, query string, fromNs int64, toNs int64, stepMs int64,
	limit int64, forward bool) (chan []shared.LogEntry, bool, error) {
	conn, err := q.Session.GetDB(ctx)
	if err != nil {
		return nil, false, err
	}
	chain, err := logql_transpiler_v2.Transpile(query)
	if err != nil {
		return nil, false, err
	}
	versionInfo, err := dbVersion.GetVersionInfo(ctx, conn.Config.ClusterName != "", conn.Session)
	if err != nil {
		return nil, false, err
	}

	_ctx, cancel := context.WithCancel(ctx)

	plannerCtx := tables.PopulateTableNames(&shared.PlannerContext{
		IsCluster:  conn.Config.ClusterName != "",
		From:       time.Unix(fromNs/1000000000, 0),
		To:         time.Unix(toNs/1000000000, 0),
		OrderASC:   forward,
		Limit:      int64(limit),
		Ctx:        _ctx,
		CancelCtx:  cancel,
		CHDb:       conn.Session,
		CHFinalize: true,
		Step:       time.Duration(stepMs) * time.Millisecond,
		CHSqlCtx: &sql.Ctx{
			Params: map[string]sql.SQLObject{},
			Result: map[string]sql.SQLObject{},
		},
		VersionInfo: versionInfo,
	}, conn)
	res, err := chain[0].Process(plannerCtx, nil)
	return res, chain[0].IsMatrix(), err
}

func (q *QueryRangeService) QueryRange(ctx context.Context, query string, fromNs int64, toNs int64, stepMs int64,
	limit int64, forward bool) (chan model.QueryRangeOutput, error) {
	out, isMatrix, err := q.prepareOutput(ctx, query, fromNs, toNs, stepMs, limit, forward)
	if err != nil {
		return nil, err
	}
	res := make(chan model.QueryRangeOutput)

	if !isMatrix {
		go func() {
			q.exportStreamsValue(out, res)
		}()
		return res, nil
	}
	go func() {
		defer close(res)

		res <- model.QueryRangeOutput{Str: `{"status": "success","data": {"resultType": "matrix", "result": [`}

		var lastFp uint64
		i := 0
		j := 0

		for entries := range out {
			for _, e := range entries {
				if e.Err != nil && e.Err != io.EOF {
					onErr(e.Err, res)
					return
				}
				if e.Err == io.EOF {
					break
				}
				if i == 0 || lastFp != e.Fingerprint {
					if i > 0 {
						res <- model.QueryRangeOutput{Str: "]},"}
					}
					lastFp = e.Fingerprint
					i = 1
					j = 0
					stream, _ := json.Marshal(e.Labels)
					res <- model.QueryRangeOutput{Str: fmt.Sprintf(`{%s:%s, %s: [`,
						strconv.Quote("stream"), string(stream), strconv.Quote("values"))}
				}
				if j > 0 {
					res <- model.QueryRangeOutput{Str: ","}
				}
				j = 1
				val := strconv.FormatFloat(e.Value, 'f', -1, 64)
				if strings.Contains(val, ".") {
					val := strings.TrimSuffix(val, "0")
					val = strings.TrimSuffix(val, ".")
				}

				res <- model.QueryRangeOutput{
					Str: fmt.Sprintf(`[%f, "%s"]`, float64(e.TimestampNS)/1e9, val),
				}
			}
		}

		if i > 0 {
			res <- model.QueryRangeOutput{Str: "]}"}
		}
		res <- model.QueryRangeOutput{Str: "]}}"}
	}()
	return res, nil
}

func (q *QueryRangeService) QueryInstant(ctx context.Context, query string, timeNs int64, stepMs int64,
	limit int64) (chan model.QueryRangeOutput, error) {
	out, isMatrix, err := q.prepareOutput(ctx, query, timeNs-300000000000, timeNs, stepMs, limit, false)
	if err != nil {
		return nil, err
	}
	res := make(chan model.QueryRangeOutput)
	if !isMatrix {
		go func() {
			q.exportStreamsValue(out, res)
		}()
		return res, nil
	}

	go func() {
		defer close(res)

		res <- model.QueryRangeOutput{Str: `{"status": "success","data": {"resultType": "vector", "result": [`}
		i := 0
		lastValues := make(map[uint64]shared.LogEntry)
		for entries := range out {
			for _, e := range entries {
				if e.Err != nil && e.Err != io.EOF {
					onErr(e.Err, res)
					return
				}
				if e.Err == io.EOF {
					break
				}
				if _, ok := lastValues[e.Fingerprint]; !ok {
					lastValues[e.Fingerprint] = e
					continue
				}
				if lastValues[e.Fingerprint].TimestampNS < e.TimestampNS {
					lastValues[e.Fingerprint] = e
					continue
				}
			}
		}
		for _, e := range lastValues {
			if i > 0 {
				res <- model.QueryRangeOutput{Str: ","}
			}
			stream, _ := json.Marshal(e.Labels)

			val := strconv.FormatFloat(e.Value, 'f', -1, 64)
			if strings.Contains(val, ".") {
				val := strings.TrimSuffix(val, "0")
				val = strings.TrimSuffix(val, ".")
			}
			res <- model.QueryRangeOutput{Str: fmt.Sprintf(
				`{"metric":%s, "value": [%d, %s]}`,
				string(stream), e.TimestampNS/1000000000, strconv.Quote(val))}
			i++
		}
		res <- model.QueryRangeOutput{Str: "]}}"}
	}()

	return res, nil
}

func (q *QueryRangeService) Tail(ctx context.Context, query string) (model.IWatcher, error) {
	if q.plugin != nil {
		return q.plugin.Tail(ctx, query)
	}

	conn, err := q.Session.GetDB(ctx)
	if err != nil {
		return nil, err
	}
	sqlQuery, err := logql_transpiler_v2.Transpile(query)
	if err != nil {
		return nil, err
	}

	res := NewWatcher(make(chan model.QueryRangeOutput))

	from := time.Now().Add(time.Minute * -5)

	_ctx, cancel := context.WithCancel(ctx)

	go func() {
		ticker := time.NewTicker(time.Second)
		defer cancel()
		defer close(res.GetRes())
		defer ticker.Stop()
		for _ = range ticker.C {
			versionInfo, err := dbVersion.GetVersionInfo(ctx, conn.Config.ClusterName != "", conn.Session)
			if err != nil {
				logger.Error(err)
				return
			}

			select {
			case <-res.Done():
				return
			default:

			}
			out, err := sqlQuery[0].Process(tables.PopulateTableNames(&shared.PlannerContext{
				IsCluster:  conn.Config.ClusterName != "",
				From:       from,
				To:         time.Now(),
				OrderASC:   false,
				Limit:      0,
				Ctx:        _ctx,
				CHDb:       conn.Session,
				CHFinalize: true,
				CHSqlCtx: &sql.Ctx{
					Params: map[string]sql.SQLObject{},
					Result: map[string]sql.SQLObject{},
				},
				CancelCtx:   cancel,
				VersionInfo: versionInfo,
			}, conn), nil)
			if err != nil {
				logger.Error(err)
				return
			}
			_res := make(chan model.QueryRangeOutput)
			go func() {
				defer close(_res)
				var lastFp uint64
				i := 0
				j := 0
				_res <- model.QueryRangeOutput{Str: "{\"streams\":["}
				for entries := range out {
					for _, e := range entries {
						if e.Err == io.EOF {
							continue
						}
						if e.Err != nil {
							onErr(e.Err, _res)
							return
						}
						if lastFp != e.Fingerprint {
							if i > 0 {
								_res <- model.QueryRangeOutput{Str: "]},"}
							}
							lastFp = e.Fingerprint
							i = 1
							j = 0
							stream, _ := json.Marshal(e.Labels)
							_res <- model.QueryRangeOutput{Str: fmt.Sprintf(`{"stream":%s, "values": [`,
								string(stream))}
						}
						if j > 0 {
							_res <- model.QueryRangeOutput{Str: ","}
						}
						j = 1
						msg, err := json.Marshal(e.Message)
						if err != nil {
							msg = []byte("error string")
						}
						_res <- model.QueryRangeOutput{
							Str: fmt.Sprintf(`["%d", %s]`, e.TimestampNS, msg),
						}
						if from.UnixNano() < e.TimestampNS {
							from = time.Unix(0, e.TimestampNS+1)
						}
					}
				}
				if i > 0 {
					_res <- model.QueryRangeOutput{Str: "]}"}
				}
				_res <- model.QueryRangeOutput{Str: "]}"}
			}()
			builder := strings.Builder{}
			for str := range _res {
				builder.WriteString(str.Str)
			}
			res.GetRes() <- model.QueryRangeOutput{Str: builder.String()}
		}
	}()
	return res, nil
}

type Watcher struct {
	res    chan model.QueryRangeOutput
	ctx    context.Context
	cancel context.CancelFunc
}

func NewWatcher(res chan model.QueryRangeOutput) model.IWatcher {
	ctx, cancel := context.WithCancel(context.Background())
	return &Watcher{
		res:    res,
		ctx:    ctx,
		cancel: cancel,
	}
}

func (w *Watcher) Done() <-chan struct{} {
	return w.ctx.Done()
}

func (w *Watcher) GetRes() chan model.QueryRangeOutput {
	return w.res
}

func (w *Watcher) Close() {
	w.cancel()
}
