package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"time"
)

// https://docs.datadoghq.com/tracing/guide/send_traces_to_agent_by_api/#model
type DataDogModel struct {
	Duration int64              `json:"duration"`
	Error    int32              `json:"error"`
	Meta     map[string]string  `json:"meta"`
	Metrics  map[string]float64 `json:"metrics"`
	Name     string             `json:"name"`
	ParentID int64              `json:"parent_id"`
	Resource string             `json:"resource"`
	Service  string             `json:"service"`
	SpanID   int64              `json:"span_id"`
	Start    int64              `json:"start"`
	TraceID  int64              `json:"trace_id"`
	Type     DataDogModelEnum   `json:"type"`
}

type DataDogModelEnum string

func (d DataDogModelEnum) String() string {
	return string(d)
}

const (
	DataDogModelEnumWeb    DataDogModelEnum = "web"
	DataDogModelEnumDb     DataDogModelEnum = "db"
	DataDogModelEnumCache  DataDogModelEnum = "cache"
	DataDogModelEnumCustom DataDogModelEnum = "custom"
)

type DatadogReq [][]DataDogModel

func (d DatadogReq) Serialize() ([]byte, error) {
	return json.Marshal(d)
}

func NewDatadogSender(opts LogSenderOpts) ISender {
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	path := "/v0.3/traces"
	if os.Getenv("DPATH") != "" {
		path = os.Getenv("DPATH")
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
	l.generate = func() IRequest {

		var (
			spansPerTrace int = 3
			traces        int = 0
			remainder     int = 0
		)

		// if the total is less than the spans per trace, we only do those.
		if opts.LinesPS < spansPerTrace {
			remainder = opts.LinesPS
		} else {
			traces = opts.LinesPS / spansPerTrace
			remainder = opts.LinesPS % spansPerTrace
		}

		// make sure we always have an array with the correct amount of slots.
		arrayLength := traces
		if remainder != 0 {
			arrayLength++
		}

		// initiate the main container
		req := make(DatadogReq, arrayLength)
		// add the traces that fit
		for i := 0; i < traces; i++ {
			req[i] = trace(i, spansPerTrace, rnd, pickCont)
		}

		// add a last trace with the remaining spans.
		if remainder != 0 {
			req[traces] = trace(traces, remainder, rnd, pickCont)
		}

		return req
	}
	return l
}

func trace(i int, spans int, rnd *rand.Rand, pickCont func() string) []DataDogModel {
	var (
		traceID = rnd.Int63n(10000000)
		tr      = make([]DataDogModel, spans)
	)

	for j := 0; j < spans; j++ {
		cont := pickCont()
		now := time.Now()

		tr[j] = DataDogModel{
			Duration: time.Duration(1 * (i + 1)).Nanoseconds(),
			Error:    0,
			Meta: map[string]string{
				"sender":          "longtest",
				"randomContainer": cont,
			},
			Metrics: map[string]float64{
				REQ_BYTES: rnd.Float64(),
			},
			Name:     fmt.Sprintf("longtest-%d-%d", i+1, j+1),
			ParentID: 0,
			Resource: "/",
			Service:  "longtest",
			SpanID:   int64((i + 1) * (j + 1)),
			Start:    now.UnixNano(),
			TraceID:  traceID,
			Type:     DataDogModelEnumWeb,
		}
	}

	return tr
}
