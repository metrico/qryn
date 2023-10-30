package shared

import (
	"wasm_parts/sql_select"
)

type RequestProcessor interface {
	IsMatrix() bool
	Process(*PlannerContext, chan []LogEntry) (chan []LogEntry, error)
}

type SQLRequestPlanner interface {
	Process(ctx *PlannerContext) (sql.ISelect, error)
}

type LogEntry struct {
	TimestampNS int64
	Fingerprint uint64
	Labels      map[string]string
	Message     string
	Value       float64

	Err error
}

type RequestProcessorChain []RequestProcessor

type RequestPlanner interface {
	Process(cnain RequestProcessorChain) (RequestProcessorChain, error)
}
