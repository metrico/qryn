package controller

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/metrico/qryn/v5/shared/samplesconfig"
	"github.com/metrico/qryn/v5/writer/service"
	"github.com/metrico/qryn/v5/writer/service/registry"
	"github.com/metrico/qryn/v5/writer/utils"
	"github.com/metrico/qryn/v5/writer/utils/helpers"
	"github.com/metrico/qryn/v5/writer/utils/promise"
)

func TestResolveServices_RequireRegistry(t *testing.T) {
	// With a nil Registry each resolver must return an error, not panic.
	old := Registry
	Registry = nil
	defer func() { Registry = old }()
	for _, fn := range []func(string) (InsertServices, error){
		ResolveTraceServices, ResolveLogServices, ResolveProfileServices,
	} {
		if _, err := fn("dsn"); err == nil {
			t.Fatal("expected error when Registry is nil")
		}
	}
}

// namedSvc is a no-op insert service with a fixed node name, for asserting
// which node a resolver picks.
type namedSvc struct{ node string }

func (s *namedSvc) Request(helpers.SizeGetter, int) *promise.Promise[uint32] {
	return promise.Fulfilled[uint32](nil, 0)
}
func (s *namedSvc) Run()                     {}
func (s *namedSvc) Stop()                    {}
func (s *namedSvc) Ping() (time.Time, error) { return time.Time{}, nil }
func (s *namedSvc) GetState(int) int         { return 0 }
func (s *namedSvc) GetNodeName() string      { return s.node }
func (s *namedSvc) Init()                    {}
func (s *namedSvc) PlanFlush()               {}

// TestResolveLogServicesNodeFollowsTimeSeries pins Node to the time-series
// service's node. Node namespaces the fingerprint cache (FPCache.DB(Node) in
// IngestParsed), and that cache gates exactly one thing: whether a series'
// time_series row is appended — rows that are pushed to Ts. With an empty
// DSN the static registry picks an independent random service per getter
// call, so samples and time series can resolve to different nodes within one
// request; keying the cache to the samples node would then mark fingerprints
// seen on a node that never received the time_series row, silently losing it
// there until the cache TTL resets.
func TestResolveLogServicesNodeFollowsTimeSeries(t *testing.T) {
	old := Registry
	Registry = registry.NewStaticServiceRegistry(registry.StaticServiceRegistryOpts{
		SamplesSvcs: map[string]service.IInsertServiceV2{
			"spl-node": &namedSvc{node: "spl-node"},
		},
		TimeSeriesSvcs: map[string]service.IInsertServiceV2{
			"ts-node": &namedSvc{node: "ts-node"},
		},
	})
	t.Cleanup(func() { Registry = old })

	svcs, err := ResolveLogServices("")
	if err != nil {
		t.Fatal(err)
	}
	if svcs.Node != "ts-node" {
		t.Errorf("Node=%q, want %q: the fingerprint cache must be namespaced by the node receiving time_series rows", svcs.Node, "ts-node")
	}
}

// reloadSamplesConfig re-reads the samples environment inside a test. Init is
// once-guarded for production; tests drive the loader directly.
func reloadSamplesConfig(t *testing.T) {
	t.Helper()
	if err := samplesconfig.Reload(); err != nil {
		t.Fatalf("samplesconfig reload: %v", err)
	}
	t.Cleanup(func() {
		t.Setenv("SAMPLES_SPLIT_BY_SIGNAL", "false")
		_ = samplesconfig.Reload()
	})
}

// installRegistry points the package-global Registry at recorder services and
// restores the previous one on cleanup.
func installRegistry(t *testing.T, spl, ts, mtr service.IInsertServiceV2) {
	t.Helper()
	old := Registry
	Registry = &metricsFakeRegistry{samples: spl, timeSeries: ts, mtr: mtr}
	t.Cleanup(func() { Registry = old })
}

// With the split off, metrics keep going to the samples service and Mtr stays
// nil, so doPush skips it: the shared table is still the only destination.
func TestResolveLogServicesNoSplit(t *testing.T) {
	t.Setenv("SAMPLES_SPLIT_BY_SIGNAL", "false")
	reloadSamplesConfig(t)

	spl := &recorderSvc{}
	ts := &recorderSvc{}
	mtr := &recorderSvc{}
	installRegistry(t, spl, ts, mtr)

	svcs, err := ResolveLogServices("")
	if err != nil {
		t.Fatalf("ResolveLogServices: %v", err)
	}
	if svcs.Spl != spl {
		t.Error("Spl not resolved to the samples service")
	}
	if svcs.Mtr != nil {
		t.Error("Mtr must stay nil without the split")
	}
}

// With the split on, a metrics service must be resolved: it is the only path to
// samples_metrics, and IngestParsed pushes the metric half at it.
func TestResolveLogServicesWithSplit(t *testing.T) {
	t.Setenv("SAMPLES_SPLIT_BY_SIGNAL", "true")
	reloadSamplesConfig(t)

	spl := &recorderSvc{}
	ts := &recorderSvc{}
	mtr := &recorderSvc{}
	installRegistry(t, spl, ts, mtr)

	svcs, err := ResolveLogServices("")
	if err != nil {
		t.Fatalf("ResolveLogServices: %v", err)
	}
	if svcs.Spl != spl {
		t.Error("Spl not resolved to the samples service")
	}
	if svcs.Mtr != mtr {
		t.Error("Mtr not resolved to the metrics service")
	}
}

// ResolveMetricServices delegates, so it must expose the same pair.
func TestResolveMetricServicesWithSplit(t *testing.T) {
	t.Setenv("SAMPLES_SPLIT_BY_SIGNAL", "true")
	reloadSamplesConfig(t)

	spl := &recorderSvc{}
	ts := &recorderSvc{}
	mtr := &recorderSvc{}
	installRegistry(t, spl, ts, mtr)

	svcs, err := ResolveMetricServices("")
	if err != nil {
		t.Fatalf("ResolveMetricServices: %v", err)
	}
	if svcs.Spl != spl || svcs.Mtr != mtr {
		t.Error("ResolveMetricServices must expose both halves")
	}
}

// doParse rebuilds InsertServices from context keys rather than from what
// Resolve*Services returned, so the metrics service needs a key of its own.
// Without it every HTTP ingest route hands IngestParsed a nil Mtr and doPush
// silently drops the metric half.
func TestMetricsServiceSurvivesTheContext(t *testing.T) {
	mtr := &recorderSvc{}
	req := httptest.NewRequest(http.MethodPost, "/", nil)
	ctx := context.WithValue(req.Context(), utils.ContextKeyMtrService,
		service.IInsertServiceV2(mtr))
	req = req.WithContext(ctx)

	if got := getService(req, utils.ContextKeyMtrService); got != mtr {
		t.Errorf("getService returned %v, want the metrics service", got)
	}
}
