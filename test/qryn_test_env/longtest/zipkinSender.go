package main

import (
	"encoding/json"
	"fmt"
	"github.com/openzipkin/zipkin-go/model"
	"math/rand"
	"net"
	"os"
	"time"
)

type ZipkinReq []model.SpanModel

func (z ZipkinReq) Serialize() ([]byte, error) {
	return json.Marshal(z)
}

func NewZipkinSender(opts LogSenderOpts) ISender {
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
		return l.pickRandom(opts.Containers)
	}
	pickLine := func() string {
		return l.pickRandom(opts.Lines)
	}
	l.generate = func() IRequest {
		req := make(ZipkinReq, opts.LinesPS)
		high := rnd.Uint64()

		for i := 0; i < opts.LinesPS; i++ {
			req[i] = model.SpanModel{
				SpanContext: model.SpanContext{
					TraceID: model.TraceID{
						High: high,
						Low:  uint64(i / 100),
					},
					ID: model.ID(i + 1),
				},
				Name:      "longtest",
				Timestamp: time.Now(),
				Duration:  1000,
				LocalEndpoint: &model.Endpoint{
					ServiceName: "longtest-service",
					IPv4:        net.IPv4(192, 168, 0, 1),
					IPv6:        nil,
					Port:        8080,
				},
				Annotations: []model.Annotation{
					{time.Now(), pickLine()},
				},
				Tags: map[string]string{
					"sender":          "longtest",
					"randomContainer": pickCont(),
					"randomFloat":     fmt.Sprintf("%f", 50+(rand.Float64()*100-50)),
				},
			}
		}
		return req
	}
	return l
}
