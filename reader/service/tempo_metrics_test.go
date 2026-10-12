package service

import (
	"testing"
	"time"

	"github.com/metrico/qryn/v5/reader/model"
	traceql_ir "github.com/metrico/qryn/v5/reader/traceql/traceql_ir"
	"github.com/stretchr/testify/require"
)

func TestMetricsExtractAllFilters(t *testing.T) {
	for _, c := range []struct {
		query           string
		keys, ops, vals []string
	}{
		{`{} | rate()`, nil, nil, nil},
		{`{.a = "x" && duration > 1ms} | rate()`,
			[]string{".a", "duration"}, []string{"=", ">"}, []string{"x", "1ms"}},
		{`{ false } | rate()`, []string{"__false__"}, []string{"="}, []string{"0"}},
		{`{ true && .a } | rate()`, nil, nil, nil},
		{`({.a = 1} || {.b = 2} &>> {status = error}) | rate()`,
			[]string{".a", ".b", "status"}, []string{"=", "=", "="}, []string{"1", "2", "error"}},
	} {
		t.Run(c.query, func(t *testing.T) {
			q, err := traceql_ir.Parse(c.query)
			require.NoError(t, err)
			keys, ops, vals := metricsExtractAllFilters(q)
			require.Equal(t, c.keys, keys)
			require.Equal(t, c.ops, ops)
			require.Equal(t, c.vals, vals)
		})
	}
}

func TestMetricsCompareRequestFilters(t *testing.T) {
	q, err := traceql_ir.Parse(`{.a = "x"} | compare({.b = "y" || .c = 3}, 5)`)
	require.NoError(t, err)
	req := &model.MetricsQueryRequest{From: time.Unix(100, 0), To: time.Unix(200, 0)}
	got := (&TempoService{}).metricsCompareRequest(q, q.Metrics, req, "traces", "attrs")
	require.Equal(t, []string{".a"}, got.OuterKeys)
	require.Equal(t, []string{"x"}, got.OuterVals)
	require.Equal(t, []string{".b", ".c"}, got.InnerKeys)
	require.Equal(t, []string{"=", "="}, got.InnerOps)
	require.Equal(t, []string{"y", "3"}, got.InnerVals)
	require.Equal(t, 5, got.TopN)
}
