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

// tables qualifies the table names of a pass: unqualified ones resolve in
// the connection's database.
type tables struct{ db string }

func (t tables) prefix() string {
	if t.db == "" {
		return ""
	}
	return "`" + strings.NewReplacer(`\`, `\\`, "`", "\\`").Replace(t.db) + "`."
}

// replicaDelay is the seconds tempo_traces on the running node lags its
// replicas; 0 when it is not replicated.
func (t tables) replicaDelay() string {
	db := "currentDatabase()"
	if t.db != "" {
		db = quote(t.db)
	}
	return "(SELECT max(absolute_delay) FROM system.replicas WHERE database = " + db + " AND table = 'tempo_traces')"
}

func quote(s string) string {
	return "'" + strings.NewReplacer(`\`, `\\`, `'`, `\'`).Replace(s) + "'"
}

// passSelect returns the SELECT of one pass: the rows for
// tempo_traces_nested_set, in its column order. delay is an SQL expression
// for the replica delay in seconds.
func passSelect(t tables, idle time.Duration, maxSpans int, delay string) string {
	return strings.NewReplacer(
		"{DB}", t.prefix(),
		"{IDLE_MS}", strconv.FormatInt(idle.Milliseconds(), 10),
		"{MAX_SPANS}", strconv.Itoa(maxSpans),
		"{REPLICA_DELAY}", delay,
	).Replace(passSQL)
}

// passSettings are the settings the pass needs.
const passSettings = "SETTINGS enable_analyzer = 1, join_use_nulls = 0"

const nestedSetColumns = "(oid, trace_id, span_id, date, left, right, parent, version)"

// insertLocal inserts the rows of a pass SELECT into the local table.
func insertLocal(sel string) string {
	return "INSERT INTO tempo_traces_nested_set " + nestedSetColumns + "\n" + sel + "\n" + passSettings
}

// insertCluster runs a pass SELECT on every shard of cluster, each shard
// inserting its rows into its own local table.
func insertCluster(t tables, cluster, sel string) string {
	return "INSERT INTO " + t.prefix() + "tempo_traces_nested_set_dist " + nestedSetColumns + "\n" +
		"SELECT * FROM cluster(" + quote(cluster) + ", view(\n" + sel + "\n))\n" +
		passSettings + ", parallel_distributed_insert_select = 2"
}

// Pass numbers the traces idle for at least idle, oldest first, up to
// maxSpans spans; a single trace over the cap is numbered alone.
func Pass(ctx context.Context, conn clickhouse.Conn, idle time.Duration, maxSpans int) error {
	t := tables{}
	return conn.Exec(ctx, insertLocal(passSelect(t, idle, maxSpans, t.replicaDelay())))
}

// ClusterPass is Pass on every shard of cluster over the tables of db, each
// shard numbering its own traces; maxSpans applies per shard.
func ClusterPass(ctx context.Context, conn clickhouse.Conn, cluster, db string, idle time.Duration, maxSpans int) error {
	t := tables{db: db}
	return conn.Exec(ctx, insertCluster(t, cluster, passSelect(t, idle, maxSpans, t.replicaDelay())))
}
