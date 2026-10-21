package maintenance

import (
	"context"
	"strings"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/metrico/qryn/v5/ctrl/logger"
	"github.com/metrico/qryn/v5/ctrl/qryn/sql"
	"github.com/metrico/qryn/v5/shared/chtest"
)

// nestedSetStatements is the number of statements traces.sql gains for the
// nested-set tables and the touched view.
const nestedSetStatements = 3

func showCreate(t *testing.T, conn clickhouse.Conn, table string) string {
	t.Helper()
	var create string
	if err := conn.QueryRow(context.Background(), "SHOW CREATE TABLE "+table).Scan(&create); err != nil {
		t.Fatalf("%s: %v", table, err)
	}
	return create
}

func assertNestedSetSchema(t *testing.T, conn clickhouse.Conn) {
	t.Helper()
	ns := showCreate(t, conn, "tempo_traces_nested_set")
	for _, want := range []string{"ReplacingMergeTree(version)", "PARTITION BY (oid, date)", "ORDER BY (oid, trace_id, span_id)"} {
		if !strings.Contains(ns, want) {
			t.Errorf("tempo_traces_nested_set lacks %q:\n%s", want, ns)
		}
	}
	touched := showCreate(t, conn, "tempo_traces_touched")
	for _, want := range []string{"ReplacingMergeTree(inserted_at)", "ORDER BY (oid, trace_id)", "TTL"} {
		if !strings.Contains(touched, want) {
			t.Errorf("tempo_traces_touched lacks %q:\n%s", want, touched)
		}
	}
	ctx := context.Background()
	if err := conn.Exec(ctx, `INSERT INTO tempo_traces (oid, trace_id, span_id, parent_id, timestamp_ns) VALUES
		('0', unhex('0102030405060708090a0b0c0d0e0f10'), unhex('0102030405060708'), '', 1),
		('0', unhex('0102030405060708090a0b0c0d0e0f10'), unhex('0102030405060709'), '', 2)`); err != nil {
		t.Fatal(err)
	}
	var n uint64
	if err := conn.QueryRow(ctx, `SELECT count() FROM tempo_traces_touched
		WHERE trace_id = unhex('0102030405060708090a0b0c0d0e0f10') AND inserted_at > now64(3) - INTERVAL 1 MINUTE`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("one insert of two spans of one trace: %d touched rows, want 1", n)
	}
}

func TestNestedSetSchemaOnFreshInstall(t *testing.T) {
	conn, db := chtest.Open(t)
	if err := Update(conn, db, "", CLUST_MODE_SINGLE, 7, "", "", false, logger.Logger); err != nil {
		t.Fatal(err)
	}
	assertNestedSetSchema(t, conn)
}

func TestNestedSetSchemaOnUpgrade(t *testing.T) {
	conn, db := chtest.Open(t)
	stmts, err := getSQLFile(sql.TracesScript)
	if err != nil {
		t.Fatal(err)
	}
	if err := updateScripts(conn, db, "", 1, sql.LogScript, false, 7, "", "", false, logger.Logger); err != nil {
		t.Fatal(err)
	}
	previous := strings.Join(stmts[:len(stmts)-nestedSetStatements], ";\n\n")
	if err := updateScripts(conn, db, "", 2, previous, false, 7, "", "", false, logger.Logger); err != nil {
		t.Fatal(err)
	}
	if exists, err := tableExists(conn, "tempo_traces_nested_set"); err != nil || exists {
		t.Fatalf("previous schema has tempo_traces_nested_set=%v (err %v)", exists, err)
	}
	if err := Update(conn, db, "", CLUST_MODE_SINGLE, 7, "", "", false, logger.Logger); err != nil {
		t.Fatal(err)
	}
	assertNestedSetSchema(t, conn)
}

func TestNestedSetRetentionFollowsTraces(t *testing.T) {
	conn, db := chtest.Open(t)
	if err := Update(conn, db, "", CLUST_MODE_SINGLE, 7, "", "", false, logger.Logger); err != nil {
		t.Fatal(err)
	}
	if err := Rotate(conn, "", false, nil, 7, 7, "", logger.Logger); err != nil {
		t.Fatal(err)
	}
	if ns := showCreate(t, conn, "tempo_traces_nested_set"); !strings.Contains(ns, "TTL date + toIntervalDay(7)") {
		t.Errorf("tempo_traces_nested_set has no 7-day retention:\n%s", ns)
	}
}
