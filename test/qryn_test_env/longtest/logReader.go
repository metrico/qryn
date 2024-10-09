package main

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

const (
	LOG_READ_MODE_RAW           = 1
	LOG_READ_MODE_LRA           = 2
	LOG_READ_MODE_AGG_OP        = 3
	LOG_READ_MODE_UNWRAP        = 4
	LOG_READ_MODE_UNWRAP_AGG_OP = 5
)

type LogReader struct {
	Url  string
	mtx  sync.Mutex
	rand *rand.Rand
}

func NewLogReader(url string) *LogReader {
	return &LogReader{
		Url:  url,
		rand: rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (l *LogReader) ReadLogs(mode int) {
	containers := l.getValues("container")
	switch mode {
	case LOG_READ_MODE_RAW:
		l.rawRequest(containers)
		break
	case LOG_READ_MODE_LRA:
		l.logRangeAggregationRequest(containers)
		break
	case LOG_READ_MODE_AGG_OP:
		l.aggregationOperatorRequest(containers)
		break
	case LOG_READ_MODE_UNWRAP:
		l.unwrapRequest(containers)
		break
	case LOG_READ_MODE_UNWRAP_AGG_OP:
		l.unwrapAggregationOperatorRequest(containers)
		break
	}
}

func (l *LogReader) rawRequest(containers []string) {
	l.mtx.Lock()
	cnt := pickRandom(containers, l.rand)
	to := time.Now().UnixNano() - l.rand.Int63n(600000000000)
	from := to - l.rand.Int63n(3600000000000)
	l.mtx.Unlock()
	l.request(fmt.Sprintf("{sender=\"logtest\", container=\"%s\"}", cnt), from, to)
}

func (l *LogReader) logRangeAggregationRequest(containers []string) {
	l.mtx.Lock()
	cnt := pickRandom(containers, l.rand)
	to := time.Now().UnixNano() - l.rand.Int63n(600000000000)
	from := to - l.rand.Int63n(3600000000000)
	l.mtx.Unlock()
	l.request(fmt.Sprintf("rate({sender=\"logtest\", container=\"%s\"}[1m])", cnt), from, to)
}

func (l *LogReader) aggregationOperatorRequest(containers []string) {
	l.mtx.Lock()
	cnt := pickRandom(containers, l.rand)
	to := time.Now().UnixNano() - l.rand.Int63n(600000000000)
	from := to - l.rand.Int63n(3600000000000)
	l.mtx.Unlock()
	l.request(fmt.Sprintf("sum by (level) (rate({sender=\"logtest\", container=\"%s\"}[1m]))", cnt), from, to)
}

func (l *LogReader) unwrapRequest(containers []string) {
	l.mtx.Lock()
	cnt := pickRandom(containers, l.rand)
	to := time.Now().UnixNano() - l.rand.Int63n(600000000000)
	from := to - l.rand.Int63n(3600000000000)
	l.mtx.Unlock()
	l.request(fmt.Sprintf("rate({sender=\"logtest\", container=\"%s\"} | unwrap_value [1m])", cnt), from, to)
}

func (l *LogReader) unwrapAggregationOperatorRequest(containers []string) {
	l.mtx.Lock()
	cnt := pickRandom(containers, l.rand)
	to := time.Now().UnixNano() - l.rand.Int63n(600000000000)
	from := to - l.rand.Int63n(3600000000000)
	l.mtx.Unlock()
	l.request(fmt.Sprintf("sum by (sender) (rate({sender=\"logtest\", container=\"%s\"} | unwrap_value [1m]))", cnt), from, to)
}

func (l *LogReader) request(req string, from int64, to int64) {

}

func (l *LogReader) getValues(name string) []string {
	return nil
}
