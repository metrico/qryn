package registry

import (
	"github.com/metrico/qryn/writer/service"
	"math/rand"
	"sync"
	"time"
)

type staticServiceRegistry struct {
	TimeSeriesSvcs   []service.IInsertServiceV2
	SamplesSvcs      []service.IInsertServiceV2
	MetricSvcs       []service.IInsertServiceV2
	TempoSamplesSvc  []service.IInsertServiceV2
	TempoTagsSvc     []service.IInsertServiceV2
	ProfileInsertSvc []service.IInsertServiceV2
	rand             *rand.Rand
	mtx              sync.Mutex
}

func NewStaticServiceRegistry(
	TimeSeriesSvcs map[string]service.IInsertServiceV2,
	SamplesSvcs map[string]service.IInsertServiceV2,
	MetricSvcs map[string]service.IInsertServiceV2,
	TempoSamplesSvc map[string]service.IInsertServiceV2,
	TempoTagsSvc map[string]service.IInsertServiceV2,
	ProfileInsertSvc map[string]service.IInsertServiceV2) IServiceRegistry {
	res := staticServiceRegistry{
		rand: rand.New(rand.NewSource(time.Now().UnixNano())),
	}
	for _, s := range TimeSeriesSvcs {
		res.TimeSeriesSvcs = append(res.TimeSeriesSvcs, s)
	}
	for _, s := range SamplesSvcs {
		res.SamplesSvcs = append(res.SamplesSvcs, s)
	}
	for _, s := range MetricSvcs {
		res.MetricSvcs = append(res.MetricSvcs, s)
	}
	for _, s := range TempoSamplesSvc {
		res.TempoSamplesSvc = append(res.TempoSamplesSvc, s)
	}

	for _, s := range ProfileInsertSvc {
		res.ProfileInsertSvc = append(res.ProfileInsertSvc, s)
	}
	for _, s := range TempoTagsSvc {
		res.TempoTagsSvc = append(res.TempoTagsSvc, s)
	}
	return &res
}

func staticServiceRegistryGetService[T interface{ GetNodeName() string }](r *staticServiceRegistry, id string,
	svcs []T) (T, error) {
	if id != "" {
		for _, svc := range svcs {
			if svc.GetNodeName() == id {
				return svc, nil
			}
		}
	}
	r.mtx.Lock()
	defer r.mtx.Unlock()
	idx := r.rand.Intn(len(svcs))
	return svcs[idx], nil
}

func (r *staticServiceRegistry) getService(id string,
	svcs []service.IInsertServiceV2) (service.IInsertServiceV2, error) {
	return staticServiceRegistryGetService(r, id, svcs)
}

func (r *staticServiceRegistry) GetTimeSeriesService(id string) (service.IInsertServiceV2, error) {
	return r.getService(id, r.TimeSeriesSvcs)
}

func (r *staticServiceRegistry) GetSamplesService(id string) (service.IInsertServiceV2, error) {
	return r.getService(id, r.SamplesSvcs)

}

func (r *staticServiceRegistry) GetMetricsService(id string) (service.IInsertServiceV2, error) {
	return r.getService(id, r.MetricSvcs)

}

func (r *staticServiceRegistry) GetSpansService(id string) (service.IInsertServiceV2, error) {
	return r.getService(id, r.TempoSamplesSvc)

}

func (r *staticServiceRegistry) GetSpansSeriesService(id string) (service.IInsertServiceV2, error) {
	return r.getService(id, r.TempoTagsSvc)
}

func (r *staticServiceRegistry) GetProfileInsertService(id string) (service.IInsertServiceV2, error) {
	return r.getService(id, r.ProfileInsertSvc)
}
func (r *staticServiceRegistry) Run() {}

func (r *staticServiceRegistry) Stop() {}
