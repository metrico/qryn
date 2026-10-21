// Package nestedset numbers each trace's spans depth-first and stores
// nestedSetLeft, nestedSetRight and nestedSetParent in tempo_traces_nested_set.
package nestedset

import (
	"context"
	_ "embed"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

//go:embed pass.sql
var passSQL string

// Tables names the tables a pass reads and writes.
type Tables struct {
	Traces, Touched, NestedSet string
}

// LocalTables are the tables of a single node.
var LocalTables = Tables{Traces: "tempo_traces", Touched: "tempo_traces_touched", NestedSet: "tempo_traces_nested_set"}

// PassSelect returns the SELECT of one pass over t: the rows for t.NestedSet,
// in its column order.
func PassSelect(t Tables, idle time.Duration, maxSpans int) string {
	return strings.NewReplacer(
		"{TRACES}", t.Traces,
		"{TOUCHED}", t.Touched,
		"{NESTED_SET}", t.NestedSet,
		"{IDLE_MS}", strconv.FormatInt(idle.Milliseconds(), 10),
		"{MAX_SPANS}", strconv.Itoa(maxSpans),
	).Replace(passSQL)
}

// passSettings are required by the pass: recursive CTEs need the analyzer,
// and an unmatched LEFT JOIN must read defaults, not NULL.
const passSettings = "SETTINGS enable_analyzer = 1, join_use_nulls = 0"

// Pass numbers the traces idle for at least idle, oldest first, up to
// maxSpans spans; a single trace over the cap is numbered alone.
func Pass(ctx context.Context, conn clickhouse.Conn, idle time.Duration, maxSpans int) error {
	t := LocalTables
	q := "INSERT INTO " + t.NestedSet + " (oid, trace_id, span_id, date, left, right, parent, version)\n" +
		PassSelect(t, idle, maxSpans) + "\n" + passSettings
	return conn.Exec(ctx, q)
}
