package timerange

import (
	"testing"

	sql "github.com/metrico/qryn/v5/reader/utils/sql_select"
	"github.com/stretchr/testify/require"
)

func render(t *testing.T, c sql.SQLCondition) string {
	t.Helper()
	s, err := c.String(sql.DefaultCtx())
	require.NoError(t, err)
	return s
}

func TestSpanStart(t *testing.T) {
	lower, upper := SpanStart("main.timestamp_ns", 10, 20)
	require.Equal(t, "(main.timestamp_ns) >= (10)", render(t, lower))
	require.Equal(t, "(main.timestamp_ns) < (20)", render(t, upper))
}

func TestSpanStartClosed(t *testing.T) {
	lower, upper := SpanStartClosed("timestamp_ns", 10, 20)
	require.Equal(t, "(timestamp_ns) >= (10)", render(t, lower))
	require.Equal(t, "(timestamp_ns) <= (20)", render(t, upper))
}
