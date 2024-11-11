package main

import (
	"sync"
	"time"
)

type Stat struct {
	Timings map[string]int64
	mtx     sync.Mutex
}

func (s *Stat) AddTiming(timing time.Duration) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

}

func (s *Stat) getOrDefault(k string, def int64) int64 {
	if _, ok := s.Timings[k]; !ok {
		return def
	}
	return s.Timings[k]
}
