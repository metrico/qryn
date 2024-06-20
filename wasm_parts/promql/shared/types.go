package shared

import (
	"time"
	sql "wasm_parts/sql_select"
)

type RequestPlanner interface {
	Process(ctx *PlannerContext) (sql.ISelect, error)
}

type PlannerContext struct {
	IsCluster           bool
	From                time.Time
	To                  time.Time
	Step                time.Duration
	TimeSeriesTable     string
	TimeSeriesDistTable string
	TimeSeriesGinTable  string
	MetricsTable        string
	MetricsDistTable    string
}
