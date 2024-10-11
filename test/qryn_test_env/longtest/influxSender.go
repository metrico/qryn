package main

import (
	"bytes"
	influx "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api/write"
	lp "github.com/influxdata/line-protocol"
	"math/rand"
	"time"
)

type InfluxReq []*write.Point

func (i InfluxReq) Serialize() ([]byte, error) {
	var buffer bytes.Buffer
	e := lp.NewEncoder(&buffer)
	e.SetFieldTypeSupport(lp.UintSupport)
	e.FailOnFieldErr(true)
	e.SetPrecision(time.Nanosecond)
	for _, item := range i {
		_, err := e.Encode(item)
		if err != nil {
			return nil, err
		}
	}
	return buffer.Bytes(), nil
}

func NewInfluxSender(opts LogSenderOpts) ISender {
	l := &GenericSender{
		LogSenderOpts: opts,
		rnd:           rand.New(rand.NewSource(time.Now().UnixNano())),
		timeout:       time.Second,
		path:          "/influx/api/v2/write",
	}
	l.generate = func() IRequest {
		points := make(InfluxReq, opts.LinesPS)
		for i := range points {
			points[i] = influx.NewPoint("syslog", map[string]string{
				"container": l.pickRandom(l.Containers),
				"level":     l.pickRandom([]string{"info", "debug", "error"}),
				"sender":    "logtest",
				"endpoint":  "influx",
			}, map[string]interface{}{
				"message": l.pickRandom(opts.Lines),
			}, time.Now())
		}
		return points
	}
	return l
}
