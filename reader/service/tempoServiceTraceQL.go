package service

import (
	"context"
	jsoniter "github.com/json-iterator/go"
	"github.com/metrico/qryn/reader/logql/logql_transpiler_v2/shared"
	"github.com/metrico/qryn/reader/model"
	traceql_parser "github.com/metrico/qryn/reader/traceql/parser"
	traceql_transpiler "github.com/metrico/qryn/reader/traceql/transpiler"
	"github.com/metrico/qryn/reader/utils/dbVersion"
	"time"
)

func (t *TempoService) SearchTraceQL(ctx context.Context,
	q string, limit int, from time.Time, to time.Time) (chan []model.TraceInfo, error) {
	conn, err := t.Session.GetDB(ctx)
	if err != nil {
		return nil, err
	}
	script, err := traceql_parser.Parse(q)
	if err != nil {
		return nil, err
	}
	planner, err := traceql_transpiler.Plan(script)
	if err != nil {
		return nil, err
	}
	versionInfo, err := dbVersion.GetVersionInfo(ctx, conn.Config.ClusterName != "", conn.Session)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(ctx)

	var (
		tracesAttrsTable = func() string {
			stream := jsoniter.ConfigFastest.BorrowStream(nil)
			defer jsoniter.ConfigFastest.ReturnStream(stream)
			stream.WriteRaw("`")
			stream.WriteRaw(conn.Config.Name)
			stream.WriteRaw("`.tempo_traces_attrs_gin")
			return string(stream.Buffer())
		}()
		tracesAttrsDistTable = func() string {
			stream := jsoniter.ConfigFastest.BorrowStream(nil)
			defer jsoniter.ConfigFastest.ReturnStream(stream)
			stream.WriteRaw("`")
			stream.WriteRaw(conn.Config.Name)
			stream.WriteRaw("`.tempo_traces_attrs_gin_dist")
			return string(stream.Buffer())
		}()
		tracesTable = func() string {
			stream := jsoniter.ConfigFastest.BorrowStream(nil)
			defer jsoniter.ConfigFastest.ReturnStream(stream)
			stream.WriteRaw("`")
			stream.WriteRaw(conn.Config.Name)
			stream.WriteRaw("`.tempo_traces")
			return string(stream.Buffer())
		}()
		tracesDistTable = func() string {
			stream := jsoniter.ConfigFastest.BorrowStream(nil)
			defer jsoniter.ConfigFastest.ReturnStream(stream)
			stream.WriteRaw("`")
			stream.WriteRaw(conn.Config.Name)
			stream.WriteRaw("`.tempo_traces_dist")
			return string(stream.Buffer())
		}()
		//tracesAttrsTable     = fmt.Sprintf("`%s`.tempo_traces_attrs_gin", conn.Config.Name)
		//tracesAttrsDistTable = fmt.Sprintf("`%s`.tempo_traces_attrs_gin_dist", conn.Config.Name)
		//tracesTable          = fmt.Sprintf("`%s`.tempo_traces", conn.Config.Name)
		//tracesDistTable      = fmt.Sprintf("`%s`.tempo_traces_dist", conn.Config.Name)
	)

	ch, err := planner.Process(&shared.PlannerContext{
		IsCluster:            conn.Config.ClusterName != "",
		From:                 from,
		To:                   to,
		Limit:                int64(limit),
		TracesAttrsTable:     tracesAttrsTable,
		TracesAttrsDistTable: tracesAttrsDistTable,
		TracesTable:          tracesTable,
		TracesDistTable:      tracesDistTable,
		Ctx:                  ctx,
		CHDb:                 conn.Session,
		CancelCtx:            cancel,
		VersionInfo:          versionInfo,
	})

	if err != nil {
		return nil, err
	}
	res := make(chan []model.TraceInfo)
	go func() {
		defer close(res)
		defer cancel()
		for ch := range ch {
			res <- ch
		}
	}()
	return res, nil
}
