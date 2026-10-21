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

// PassSelect returns the SELECT of one pass over the local tables: the rows
// for tempo_traces_nested_set, in its column order.
func PassSelect(idle time.Duration, maxSpans int) string {
	return strings.NewReplacer(
		"{IDLE_MS}", strconv.FormatInt(idle.Milliseconds(), 10),
		"{MAX_SPANS}", strconv.Itoa(maxSpans),
	).Replace(passSQL)
}

// passSettings are the settings the pass needs.
const passSettings = "SETTINGS enable_analyzer = 1, join_use_nulls = 0"

// Pass numbers the traces idle for at least idle, oldest first, up to
// maxSpans spans; a single trace over the cap is numbered alone.
func Pass(ctx context.Context, conn clickhouse.Conn, idle time.Duration, maxSpans int) error {
	q := "INSERT INTO tempo_traces_nested_set (oid, trace_id, span_id, date, left, right, parent, version)\n" +
		PassSelect(idle, maxSpans) + "\n" + passSettings
	return conn.Exec(ctx, q)
}
