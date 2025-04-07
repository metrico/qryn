package model

import (
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

type ILabelsGetter interface {
	Get(fp uint64) labels.Labels
}

type SeriesSet struct {
	Error  error
	Series []*Series
	idx    int
}

func (e *SeriesSet) Reset() {
	e.idx = -1
}

func (e *SeriesSet) Err() error {
	return e.Error
}

func (e *SeriesSet) Next() bool {
	e.idx++
	return e.Series != nil && e.idx < len(e.Series)
}

func (e *SeriesSet) At() storage.Series {
	return e.Series[e.idx]
}

func (e *SeriesSet) Warnings() storage.Warnings {
	return nil
}

type Sample struct {
	TimestampMs int64
	Value       float64
}

type Series struct {
	LabelsGetter ILabelsGetter
	Fp           uint64
	Samples      []Sample
}

func (s *Series) Labels() labels.Labels {
	return s.LabelsGetter.Get(s.Fp)
}

func (s *Series) Iterator() chunkenc.Iterator {
	return &seriesIt{
		samples: s.Samples,
		idx:     -1,
	}
}

type seriesIt struct {
	samples []Sample
	idx     int
}

func (s *seriesIt) Next() bool {
	s.idx++
	return s.idx < len(s.samples)
}

func (s *seriesIt) Seek(t int64) bool {
	l := 0
	u := len(s.samples)
	idx := int(0)
	if t <= s.samples[0].TimestampMs {
		s.idx = 0
		return true
	}
	for u > l {
		idx = (u + l) / 2
		if s.samples[idx].TimestampMs == t {
			l = idx
			break
		}
		if s.samples[idx].TimestampMs < t {
			l = idx + 1
			continue
		}
		u = idx
	}
	s.idx = idx
	return s.idx < len(s.samples)
}

func (s *seriesIt) At() (int64, float64) {
	return s.samples[s.idx].TimestampMs, s.samples[s.idx].Value
}

func (s *seriesIt) Err() error {
	return nil
}
