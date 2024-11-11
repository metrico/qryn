package main

import (
	"github.com/openzipkin/zipkin-go/model"
	"math/rand"
	"net"
	"os"
	"time"
)

func NewSGSender(opts LogSenderOpts) ISender {
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	path := "/tempo/spans"
	if os.Getenv("ZPATH") != "" {
		path = os.Getenv("ZPATH")
	}
	l := &GenericSender{
		LogSenderOpts: opts,
		rnd:           rnd,
		timeout:       time.Second,
		path:          path,
	}
	pickCont := func() string {
		return l.pickRandom(opts.Containers[:5])
	}
	l.generate = func() IRequest {
		req := make(ZipkinReq, opts.LinesPS)
		high := rnd.Uint64()
		dur := uint64(rnd.Float64() * 1000)

		for i := 0; i < opts.LinesPS; i += 2 {
			client := "test2-" + pickCont()
			server := "test2-" + pickCont()
			req[i] = model.SpanModel{
				SpanContext: model.SpanContext{
					TraceID: model.TraceID{
						High: high,
						Low:  uint64(i / 100),
					},
					ID: model.ID(i + 1),
				},
				Name:      "longtest-SG",
				Timestamp: time.Now(),
				Duration:  time.Duration(dur) * time.Microsecond,
				Kind:      model.Client,
				LocalEndpoint: &model.Endpoint{
					ServiceName: client,
					IPv4:        net.IPv4(192, 168, 0, 1),
					IPv6:        nil,
					Port:        8080,
				},
				RemoteEndpoint: &model.Endpoint{
					ServiceName: server,
					IPv4:        net.IPv4(192, 168, 0, 2),
					IPv6:        nil,
					Port:        8080,
				},
				Tags: map[string]string{
					"sender": "longtest-SG",
				},
			}
			req[i+1] = model.SpanModel{
				SpanContext: model.SpanContext{
					TraceID: model.TraceID{
						High: high,
						Low:  uint64(i / 100),
					},
					ID:       model.ID(i + 2),
					ParentID: &[]model.ID{model.ID(i + 1)}[0],
				},
				Name:      "longtest-SG",
				Timestamp: time.Now(),
				Duration:  time.Duration(dur/2) * time.Microsecond,
				Kind:      model.Server,
				LocalEndpoint: &model.Endpoint{
					ServiceName: server,
					IPv4:        net.IPv4(192, 168, 0, 2),
					IPv6:        nil,
					Port:        8080,
				},
				RemoteEndpoint: &model.Endpoint{
					ServiceName: client,
					IPv4:        net.IPv4(192, 168, 0, 1),
					IPv6:        nil,
					Port:        8080,
				},
				Tags: map[string]string{
					"sender": "longtest-SG",
				},
			}
		}
		return req
	}
	return l
}
