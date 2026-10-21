package nestedset

import (
	"context"
	"fmt"
	"maps"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/metrico/qryn/v5/ctrl/logger"
	"github.com/metrico/qryn/v5/ctrl/qryn/maintenance"
	"github.com/metrico/qryn/v5/shared/chtest"
)

// hostRows returns host -> "trace/span" -> numbers of the rows a query over
// clusterAllReplicas returns; columns: host, trace, span, left, right, parent.
func hostRows(t *testing.T, conn clickhouse.Conn, q string) map[string]map[string]numbers {
	t.Helper()
	rows, err := conn.Query(context.Background(), q)
	if err != nil {
		t.Fatalf("%v\n%s", err, q)
	}
	defer rows.Close()
	got := map[string]map[string]numbers{}
	for rows.Next() {
		var host string
		var trace, id uint64
		var n numbers
		if err := rows.Scan(&host, &trace, &id, &n.left, &n.right, &n.parent); err != nil {
			t.Fatal(err)
		}
		if got[host] == nil {
			got[host] = map[string]numbers{}
		}
		got[host][fmt.Sprintf("%d/%d", trace, id)] = n
	}
	return got
}

func TestClusterPassNumbersEachShardsTracesOnThatShard(t *testing.T) {
	conn, db, cluster := chtest.OpenCluster(t)
	mode := maintenance.CLUST_MODE_SINGLE | maintenance.CLUST_MODE_DISTRIBUTED
	if err := maintenance.Update(conn, db, cluster, mode, 7, "", "", false, logger.Logger); err != nil {
		t.Fatal(err)
	}
	// Chains of 3: numbers 1/6/-1, 2/5/1, 3/4/2.
	for trace := 1; trace <= 20; trace++ {
		insertInto(t, conn, "tempo_traces_dist", chain(trace, 3)...)
	}
	exec(t, conn, "SYSTEM FLUSH DISTRIBUTED tempo_traces_dist")
	ctx := clickhouse.Context(context.Background(), clickhouse.WithSettings(clickhouse.Settings{"log_comment": db}))
	if err := (ClusterStore{Conn: conn, Cluster: cluster, Database: db}).Pass(ctx, 0, 1000); err != nil {
		t.Fatal(err)
	}

	spans := hostRows(t, conn, fmt.Sprintf(`
		SELECT hostName(), reinterpretAsUInt64(reverse(substring(trace_id, 9, 8))), reinterpretAsUInt64(reverse(span_id)),
		       toInt32(reinterpretAsUInt64(reverse(span_id))), toInt32(7 - reinterpretAsUInt64(reverse(span_id))),
		       if(parent_id = '', toInt32(-1), toInt32(reinterpretAsUInt64(reverse(substring(parent_id, 1, 8)))))
		FROM clusterAllReplicas('%s', %s.tempo_traces)`, cluster, db))
	if len(spans) < 2 {
		t.Fatalf("spans on %d hosts, want every shard", len(spans))
	}
	numbered := hostRows(t, conn, fmt.Sprintf(`
		SELECT hostName(), reinterpretAsUInt64(reverse(substring(trace_id, 9, 8))), reinterpretAsUInt64(reverse(span_id)),
		       argMax(left, version), argMax(right, version), argMax(parent, version)
		FROM clusterAllReplicas('%s', %s.tempo_traces_nested_set) GROUP BY 1, 2, 3`, cluster, db))
	if !maps.EqualFunc(spans, numbered, func(a, b map[string]numbers) bool { return maps.Equal(a, b) }) {
		t.Errorf("numbered per host:\n%v\nwant:\n%v", numbered, spans)
	}

	// Every other shard ran the pass and inserted its own rows.
	exec(t, conn, fmt.Sprintf("SYSTEM FLUSH LOGS ON CLUSTER '%s'", cluster))
	var remote uint64
	if err := conn.QueryRow(context.Background(), fmt.Sprintf(`
		SELECT uniqExact(hostName()) FROM clusterAllReplicas('%s', system.query_log)
		WHERE log_comment = '%s' AND type = 'QueryFinish' AND query_kind = 'Insert' AND NOT is_initial_query
		  AND query LIKE '%%tempo_traces_touched%%'`,
		cluster, db)).Scan(&remote); err != nil {
		t.Fatal(err)
	}
	if int(remote) != len(spans)-1 {
		t.Errorf("%d shards ran their own insert, want %d", remote, len(spans)-1)
	}
}
