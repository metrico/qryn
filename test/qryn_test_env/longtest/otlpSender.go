package main

import (
	"fmt"
	cv1 "go.opentelemetry.io/proto/otlp/common/v1"
	rv1 "go.opentelemetry.io/proto/otlp/resource/v1"
	v1 "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
	"math/rand"
	"os"
	"time"
	"unsafe"
)

type OTLPReq v1.TracesData

func (z *OTLPReq) Serialize() ([]byte, error) {
	return proto.Marshal((*v1.TracesData)(z))
}

func NewOTLPSender(opts LogSenderOpts) ISender {
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	path := "/v1/traces"
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
	/*pickLine := func() string {
		return l.pickRandom(opts.Lines)
	}*/
	l.generate = func() IRequest {
		req := &OTLPReq{
			ResourceSpans: make([]*v1.ResourceSpans, opts.LinesPS/10),
		}
		for i := range req.ResourceSpans {
			uintTraceId := []uint64{uint64(l.random(0xFFFFFFFF)), uint64(i)}
			bTraceId := make([]byte, 16)
			copy(bTraceId, unsafe.Slice((*byte)(unsafe.Pointer(&uintTraceId[0])), 16))
			uintSpanId := uint64(l.random(0xFFFFFFFF))
			bSpanId := make([]byte, 8)
			copy(bSpanId, unsafe.Slice((*byte)(unsafe.Pointer(&uintSpanId)), 8))
			req.ResourceSpans[i] = &v1.ResourceSpans{
				Resource: &rv1.Resource{
					Attributes: []*cv1.KeyValue{
						{
							Key: "service.name",
							Value: &cv1.AnyValue{
								Value: &cv1.AnyValue_StringValue{
									StringValue: "longtest-service",
								},
							},
						},
						{
							Key: "sender",
							Value: &cv1.AnyValue{
								Value: &cv1.AnyValue_StringValue{
									StringValue: "longtest",
								},
							},
						},
						{
							Key: "type",
							Value: &cv1.AnyValue{
								Value: &cv1.AnyValue_StringValue{
									StringValue: "otlp",
								},
							},
						},
					},
					DroppedAttributesCount: 0,
				},
				ScopeSpans: []*v1.ScopeSpans{
					{
						Spans: make([]*v1.Span, 10),
					},
				},
			}
			for j := range req.ResourceSpans[i].ScopeSpans[0].Spans {
				kind := v1.Span_SPAN_KIND_CLIENT
				if j%2 == 0 {
					kind = v1.Span_SPAN_KIND_SERVER
				}
				req.ResourceSpans[i].ScopeSpans[0].Spans[j] = &v1.Span{
					TraceId:      bTraceId,
					SpanId:       bSpanId,
					ParentSpanId: nil,
					Name:         "longtest",
					Kind:         kind,
					StartTimeUnixNano: uint64(time.Now().
						Add(time.Millisecond * time.Duration(-1*(l.random(500)))).
						UnixNano()),
					EndTimeUnixNano: uint64(time.Now().UnixNano()),
					Attributes: []*cv1.KeyValue{
						{
							Key: "container",
							Value: &cv1.AnyValue{
								Value: &cv1.AnyValue_StringValue{
									StringValue: pickCont(),
								},
							},
						},
						{
							Key: "randomFloat",
							Value: &cv1.AnyValue{
								Value: &cv1.AnyValue_StringValue{
									StringValue: fmt.Sprintf("%f", 50+(rand.Float64()*100-50)),
								},
							},
						},
					},
					DroppedAttributesCount: 0,
					Events:                 nil,
					DroppedEventsCount:     0,
					Links:                  nil,
					DroppedLinksCount:      0,
					Status:                 nil,
				}
			}

		}
		return req
	}
	return l
}
