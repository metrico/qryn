package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

func NewJSONConsistencyChecker(opts LogSenderOpts) ISender {
	res := &GenericSender{
		LogSenderOpts: opts,
		mtx:           sync.Mutex{},
		rnd:           rand.New(rand.NewSource(time.Now().UnixNano())),
		timeout:       time.Second,
		path:          "/loki/api/v1/push",
		numOfSends:    10,
	}
	res.generate = func() IRequest {
		logLen := 0
		req := &LogRequest{}
		for logLen < 10 {
			streamLen := 2
			stream := &LogStream{
				Stream: map[string]string{
					"container": res.pickRandom(res.Containers),
					"level":     res.pickRandom([]string{"info", "debug", "error"}),
					"sender":    "consistency-checker",
				},
				Values: make([][]interface{}, streamLen),
			}
			for i := 0; i < streamLen; i++ {
				t := fmt.Sprintf("%d", time.Now().UnixNano())
				line, _ := json.Marshal(stream.Stream)
				line = append(line, []byte(", t="+t)...)
				stream.Values[i] = []interface{}{t, string(line)}
				logLen++
			}
			req.Streams = append(req.Streams, stream)
		}
		return req
	}
	return res
}
