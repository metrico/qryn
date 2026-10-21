package nestedset

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/metrico/qryn/v5/ctrl/logger"
)

// Store runs passes against ClickHouse.
type Store interface {
	Version(ctx context.Context) (string, error)
	Pass(ctx context.Context, idle time.Duration, maxSpans int) error
}

// ConnStore is a Store over a connection to a single node.
type ConnStore struct{ Conn clickhouse.Conn }

// Version returns the server version.
func (s ConnStore) Version(ctx context.Context) (string, error) {
	var v string
	err := s.Conn.QueryRow(ctx, "SELECT version()").Scan(&v)
	return v, err
}

// Pass runs one pass.
func (s ConnStore) Pass(ctx context.Context, idle time.Duration, maxSpans int) error {
	return Pass(ctx, s.Conn, idle, maxSpans)
}

// ClusterStore is a Store over a connection to one node of a cluster whose
// traces tables are sharded on (oid, trace_id).
type ClusterStore struct {
	Conn              clickhouse.Conn
	Cluster, Database string
}

// Version returns the server version of the connected node.
func (s ClusterStore) Version(ctx context.Context) (string, error) {
	return ConnStore{Conn: s.Conn}.Version(ctx)
}

// Pass runs one pass on every shard.
func (s ClusterStore) Pass(ctx context.Context, idle time.Duration, maxSpans int) error {
	return ClusterPass(ctx, s.Conn, s.Cluster, s.Database, idle, maxSpans)
}

// minVersion is the oldest ClickHouse the pass runs on.
var minVersion = [2]int{24, 9}

// SupportedVersion reports whether a ClickHouse version string is at least minVersion.
func SupportedVersion(version string) bool {
	parts := strings.SplitN(version, ".", 3)
	if len(parts) < 2 {
		return false
	}
	major, err1 := strconv.Atoi(parts[0])
	minor, err2 := strconv.Atoi(parts[1])
	if err1 != nil || err2 != nil {
		return false
	}
	return major > minVersion[0] || major == minVersion[0] && minor >= minVersion[1]
}

// Numberer runs a pass every interval until stopped.
type Numberer struct {
	cancel context.CancelFunc
	done   sync.WaitGroup
}

// Start checks the server version and starts the passes. Below 24.9, or when
// the version can't be read, it logs one error and returns nil.
func Start(ctx context.Context, store Store, cfg Config, log logger.ILogger) *Numberer {
	version, err := store.Version(ctx)
	if err != nil {
		log.Error("nested-set numberer not started: reading the ClickHouse version: ", err.Error())
		return nil
	}
	if !SupportedVersion(version) {
		log.Error(fmt.Sprintf("nested-set numberer not started: it needs ClickHouse %d.%d or newer, the server runs %s",
			minVersion[0], minVersion[1], version))
		return nil
	}
	ctx, cancel := context.WithCancel(ctx)
	n := &Numberer{cancel: cancel}
	n.done.Add(1)
	go func() {
		defer n.done.Done()
		ticker := time.NewTicker(cfg.Interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
			if err := store.Pass(ctx, cfg.Idle, cfg.MaxSpans); err != nil && ctx.Err() == nil {
				log.Error("nested-set numberer pass: ", err.Error())
			}
		}
	}()
	log.Info(fmt.Sprintf("nested-set numberer started (idle %s, interval %s, max spans %d)", cfg.Idle, cfg.Interval, cfg.MaxSpans))
	return n
}

// Stop cancels the running pass and waits for the loop to end.
func (n *Numberer) Stop() {
	n.cancel()
	n.done.Wait()
}
