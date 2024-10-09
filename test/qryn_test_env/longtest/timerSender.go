package main

import (
	"github.com/prometheus/prometheus/prompb"
	"math/rand"
	"sync"
	"time"
)

func NewTimeSender(opts LogSenderOpts) ISender {
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
			req := make(PromReq, l.LinesPS)
			for i := 0; i < l.LinesPS; i++ {
				container := l.Containers[i%len(l.Containers)]
				req[i] = prompb.TimeSeries{
					Labels: []prompb.Label{
						{Name: "orgid", Value: opts.Headers["X-Scope-OrgID"]},
						{Name: "__name__", Value: "current_time"},
						{Name: "container", Value: container},
						{Name: "sender", Value: "logmetrics"},
					},
					Samples: []prompb.Sample{
						{
							Timestamp: time.Now().UnixMilli(),
							Value:     float64(time.Now().Unix()),
						},
					},
				}
			}
			return req
		},
	}
	return l
}
