package main

import (
	"math"
	"strings"
	"sync"
	"time"
)

type Stats struct {
	stats []map[string]int64
	mtx   sync.Mutex
}

var stats = func() *Stats {
	res := &Stats{
		stats: make([]map[string]int64, 1),
	}
	res.stats[0] = make(map[string]int64, 10)
	t := time.NewTicker(time.Second)
	go func() {
		for range t.C {
			res.mtx.Lock()
			res.stats = append(res.stats, make(map[string]int64, 10))
			if len(res.stats) > 5 {
				res.stats = res.stats[1:]
			}
			res.mtx.Unlock()
		}
	}()
	return res
}()

func (s *Stats) getOrDefault2(m map[string]int64, name string, def int64) int64 {
	if val, ok := m[name]; ok {
		return val
	}
	return def
}

func (s *Stats) getOrDefault(name string, def int64) int64 {
	return s.getOrDefault2(s.stats[len(s.stats)-1], name, def)
}

func (s *Stats) Inc(name string) {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	s.stats[len(s.stats)-1][name] = s.getOrDefault(name, 0) + 1
}

func (s *Stats) Observe(name string, val int64) {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	min := s.getOrDefault(name+"_min", math.MaxInt64)
	if min > val {
		min = val
	}
	max := s.getOrDefault(name+"_max", math.MinInt64)
	if max < val {
		max = val
	}
	count := s.getOrDefault(name+"_count", 0) + 1
	sum := s.getOrDefault(name+"_sum", 0) + val
	idx := len(s.stats) - 1
	s.stats[idx][name+"_min"] = min
	s.stats[idx][name+"_max"] = max
	s.stats[idx][name+"_count"] = count
	s.stats[idx][name+"_sum"] = sum
}

func (s *Stats) Collect() map[string]int64 {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	res := make(map[string]int64, 10)
	for _, stats := range s.stats {
		for k, v := range stats {
			if strings.HasSuffix(k, "_min") {
				a := s.getOrDefault2(res, k, math.MaxInt64)
				if a < v {
					res[k] = a
				} else {
					res[k] = v
				}
				continue
			}
			if strings.HasSuffix(k, "_max") {
				a := s.getOrDefault2(res, k, math.MinInt64)
				if a > v {
					res[k] = a
				} else {
					res[k] = v
				}
				continue
			}
			res[k] = s.getOrDefault2(res, k, 0) + v
		}
	}
	return res
}
