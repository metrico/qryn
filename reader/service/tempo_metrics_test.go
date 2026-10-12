package service

import (
	"testing"
	"time"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	traceql_ir "github.com/metrico/qryn/v5/reader/traceql/traceql_ir"
	"github.com/stretchr/testify/require"
)

func TestMetricsCompareRequest(t *testing.T) {
	ctx := &shared.PlannerContext{
		From:                 time.Unix(100, 0),
		To:                   time.Unix(200, 0),
		TracesAttrsTable:     "attrs",
		TracesAttrsDistTable: "attrs_dist",
	}
	q, err := traceql_ir.Parse(`{.a = "x"} | compare({.b = "y" || .c = 3}, 5)`)
	require.NoError(t, err)
	got := compareRequest(q.Metrics, ctx)
	require.Equal(t, 5, got.TopN)
	require.Equal(t, int64(100e9), got.BaselineFromNS)
	require.Equal(t, int64(200e9), got.BaselineToNS)
	require.Equal(t, "attrs", got.AttrsTable)

	q, err = traceql_ir.Parse(`{} | compare({.b = "y"}, 2, 50000000000, 150000000000)`)
	require.NoError(t, err)
	ctx.IsCluster = true
	got = compareRequest(q.Metrics, ctx)
	require.Equal(t, int64(50e9), got.BaselineFromNS)
	require.Equal(t, int64(150e9), got.BaselineToNS)
	require.Equal(t, int64(100e9), got.FromNS)
	require.Equal(t, "attrs_dist", got.AttrsTable)
}
