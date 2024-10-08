package main

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

type LogSender struct {
	LogSenderOpts
	mtx    sync.Mutex
	rnd    *rand.Rand
	ticker *time.Ticker
	path   string
}

func NewLogSender(opts LogSenderOpts) ISender {
	var l *GenericSender
	l = &GenericSender{
		LogSenderOpts: opts,
		mtx:           sync.Mutex{},
		rnd:           rand.New(rand.NewSource(time.Now().UnixNano())),
		timeout:       time.Second,
		path:          "/loki/api/v1/push",
		generate: func() IRequest {
			logLen := 0
			req := &LogRequest{}
			for logLen < l.LinesPS {
				streamLen := 20
				stream := &LogStream{
					Stream: map[string]string{
						"orgid":        opts.Headers["X-Scope-OrgID"],
						"container":    l.pickRandom(l.Containers),
						"level":        l.pickRandom([]string{"info", "debug", "error"}),
						"sender":       "logtest",
						"__name__":     "logs",
						"__ttl_days__": "25",
					},
					Values: make([][]interface{}, streamLen),
				}
				for i := 0; i < streamLen; i++ {
					//line := fmt.Sprintf("opaqueid=%d mos=%f", l.random(1000), float64(l.random(1000)/250))
					line := l.pickRandom(l.Lines)
					stream.Values[i] = []interface{}{fmt.Sprintf("%d", time.Now().UnixNano()), line}
					logLen++
				}
				req.Streams = append(req.Streams, stream)
			}
			return req
		},
	}
	return l
}
