package shared

import (
	"context"
	"time"
	sql "wasm_parts/sql_select"
)

type PlannerContext struct {
	IsCluster bool
	OrgID     string
	From      time.Time
	To        time.Time
	FromS     int32
	ToS       int32
	OrderASC  bool
	Limit     int64

	TimeSeriesGinTableName  string
	SamplesTableName        string
	TimeSeriesTableName     string
	TimeSeriesDistTableName string
	Metrics15sTableName     string

	TracesAttrsTable     string
	TracesAttrsDistTable string
	TracesTable          string
	TracesDistTable      string

	UseCache bool

	Ctx       context.Context
	CancelCtx context.CancelFunc

	CHFinalize bool
	CHSqlCtx   *sql.Ctx

	DDBSamplesTable string
	DDBTSTable      string

	Step time.Duration

	DeleteID string

	id int
}

func (p *PlannerContext) Id() int {
	p.id++
	return p.id
}
