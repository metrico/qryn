// Package chtest gives Go tests a fresh database on a live ClickHouse.
package chtest

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
)

// AddrEnv names the native-protocol address (host:port) of the ClickHouse the
// tests use. Tests that need it skip when it is unset.
const AddrEnv = "GIGAPIPE_TEST_CLICKHOUSE"

// Open creates an empty database and returns a connection that uses it by
// default. The database is dropped when the test ends.
func Open(t *testing.T) (clickhouse.Conn, string) {
	t.Helper()
	addr := os.Getenv(AddrEnv)
	if addr == "" {
		t.Skipf("%s is not set", AddrEnv)
	}
	ctx := context.Background()
	admin, err := clickhouse.Open(&clickhouse.Options{Addr: []string{addr}})
	if err != nil {
		t.Fatal(err)
	}
	db := fmt.Sprintf("chtest_%d", rand.Uint64())
	if err := admin.Exec(ctx, "CREATE DATABASE "+db); err != nil {
		t.Fatal(err)
	}
	conn, err := clickhouse.Open(&clickhouse.Options{Addr: []string{addr}, Auth: clickhouse.Auth{Database: db}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = conn.Close()
		_ = admin.Exec(ctx, "DROP DATABASE IF EXISTS "+db+" SYNC")
		_ = admin.Close()
	})
	return conn, db
}
