package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"sync"
	"time"
)

const (
	REQ_OK      = "req_ok"
	REQ_ERR     = "req_err"
	REQ_FAIL    = "req_fail"
	REQ_BYTES   = "req_bytes"
	REQ_TIME_MS = "req_time_ms"
)

type IRequest interface {
	Serialize() ([]byte, error)
}

type LogStream struct {
	Stream map[string]string `json:"stream"`
	Values [][]interface{}   `json:"values"`
}

type LogRequest struct {
	Streams []*LogStream `json:"streams"`
}

func (l *LogRequest) Serialize() ([]byte, error) {
	return json.Marshal(l)
}

type ISender interface {
	Run()
	Stop()
}

type LogSenderOpts struct {
	Containers []string
	Lines      []string
	LinesPS    int
	URL        string
	Headers    map[string]string
	ID         string
}

type GenericSender struct {
	LogSenderOpts
	mtx        sync.Mutex
	rnd        *rand.Rand
	ticker     *time.Ticker
	timeout    time.Duration
	path       string
	generate   func() IRequest
	numOfSends int
}

func (l *GenericSender) Run() {
	if l.ticker != nil {
		return
	}
	l.ticker = time.NewTicker(l.timeout)
	go func() {
		for range l.ticker.C {
			if l.generate == nil {
				fmt.Println("ERROR! No generate function")
			}
			numOfSends := l.numOfSends
			if numOfSends == 0 {
				numOfSends = 1
			}
			for i := 0; i < numOfSends; i++ {
				err := l.send(l.generate())
				if err != nil {
					fmt.Printf("%v\n", err)
					continue
				}
			}
		}
	}()
}

func (l *GenericSender) Stop() {
	if l.ticker != nil {
		l.ticker.Stop()
		l.ticker = nil
	}
}

func (l *GenericSender) random(n int) int {
	l.mtx.Lock()
	defer l.mtx.Unlock()
	return l.rnd.Intn(n)
}

func (l *GenericSender) pickRandom(array []string) string {
	if len(array) == 0 {
		return ""
	}
	l.mtx.Lock()
	defer l.mtx.Unlock()
	return pickRandom[string](array, l.rnd)
}

func (l *GenericSender) send(request IRequest) error {
	retries := 0
	body, err := request.Serialize()
	if err != nil {
		return err
	}
	send := func(url string, count bool) {
		if url == "" {
			url = l.URL + l.path
		}
		var statsInc = func(name string) {
			if count {
				stats.Inc(name)
			}
		}
		var statsObserve = func(name string, value int64) {
			if count {
				stats.Observe(name, value)
			}
		}
		for {
			start := time.Now()
			req, err := http.NewRequest("POST", url, bytes.NewReader(body))
			if err != nil {
				fmt.Printf("Request error: %v\n", err)
				<-time.After(time.Second)
				if retries < 10 {
					statsInc(REQ_ERR)
					retries++
					continue
				} else {
					statsInc(REQ_FAIL)
					return
				}
			}
			req.Header.Set("Content-Type", "application/json")
			for k, v := range l.Headers {
				req.Header.Set(k, v)
			}
			client := http.Client{
				Timeout: 30 * time.Second,
			}
			resp, err := client.Do(req)
			if err != nil {
				fmt.Printf("Request error: %v\n", err)
				<-time.After(time.Second)
				if retries < 10 {
					statsInc(REQ_ERR)
					retries++
					continue
				} else {
					statsInc(REQ_FAIL)
					return
				}
			}
			if resp.StatusCode/100 != 2 {
				b := bytes.Buffer{}
				io.Copy(&b, resp.Body)
				fmt.Printf("Request error: [%d]: %s\n", resp.StatusCode, string(b.Bytes()))
				<-time.After(time.Second)
				if retries < 10 {
					statsInc(REQ_ERR)
					retries++
					continue
				} else {
					stats.Inc(REQ_FAIL)
					return
				}
			}
			statsInc(REQ_OK)
			statsObserve(REQ_BYTES, int64(len(body)))
			statsObserve(REQ_TIME_MS, time.Now().Sub(start).Milliseconds())
			return
		}
	}
	go func() {
		send("", true)
	}()

	return nil
}
