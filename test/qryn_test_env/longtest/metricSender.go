package main

import (
	"github.com/golang/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/prompb"
	"hash/crc32"
	"math"
	"math/rand"
	"sync"
	"time"
)

type PromReq []prompb.TimeSeries

func (p PromReq) Serialize() ([]byte, error) {
	bytes, err := proto.Marshal(&prompb.WriteRequest{Timeseries: p})
	if err != nil {
		return nil, err
	}
	enc := snappy.Encode(nil, bytes)
	return enc, nil
}

func NewMetricSender(opts LogSenderOpts) ISender {
	var l *GenericSender
	hdrs := opts.Headers
	opts.Headers = map[string]string{}
	for k, v := range hdrs {
		opts.Headers[k] = v
	}
	opts.Headers["Content-Type"] = "application/x-protobuf"
	opts.Headers["Content-Encoding"] = "snappy"
	l = &GenericSender{
		LogSenderOpts: opts,
		mtx:           sync.Mutex{},
		rnd:           rand.New(rand.NewSource(time.Now().UnixNano())),
		timeout:       time.Second * 15,
		path:          "/api/v1/prom/remote/write",
		generate: func() IRequest {
			if opts.LinesPS/3 < len(l.Containers) {
				l.Containers = l.Containers[:opts.LinesPS/3]
			}
			req := make(PromReq, len(l.Containers)*3)
			for i, container := range l.Containers {
				base := int(crc32.ChecksumIEEE([]byte(container)))
				req[i*3] = prompb.TimeSeries{
					Labels: []prompb.Label{
						{Name: "__name__", Value: "cpu_usage"},
						{Name: "container", Value: container},
						{Name: "orgid", Value: opts.Headers["X-Scope-OrgID"]},
						{Name: "sender", Value: "logmetrics"},
					},
					Samples: []prompb.Sample{
						{
							Timestamp: time.Now().UnixMilli(),
							Value:     math.Max(float64(base%100+(l.random(20)-10)), 0),
						},
					},
				}
				req[i*3+1] = prompb.TimeSeries{
					Labels: []prompb.Label{
						{Name: "__name__", Value: "ram_usage"},
						{Name: "container", Value: container},
						{Name: "orgid", Value: opts.Headers["X-Scope-OrgID"]},
						{Name: "sender", Value: "logmetrics"},
					},
					Samples: []prompb.Sample{
						{
							Timestamp: time.Now().UnixMilli(),
							Value:     math.Max(float64(base%1000+(l.random(200)-100)), 0),
						},
					},
				}
				req[i*3+2] = prompb.TimeSeries{
					Labels: []prompb.Label{
						{Name: "__name__", Value: "network_usage"},
						{Name: "container", Value: container},
						{Name: "orgid", Value: opts.Headers["X-Scope-OrgID"]},
						{Name: "sender", Value: "logmetrics"},
					},
					Samples: []prompb.Sample{
						{
							Timestamp: time.Now().UnixMilli(),
							Value:     math.Max(float64(base%1000000+(l.random(2000)-1000)), 0),
						},
					},
				}
			}
			return req
		},
	}
	return l
}
