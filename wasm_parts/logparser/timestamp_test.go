package logparser

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func Test_containsTimestamp(t *testing.T) {
	assert.True(t, containsTimestamp("18:31:42+03"))
	assert.True(t, containsTimestamp("18:31:42-03"))
	assert.True(t, containsTimestamp("18:31:42+03:30"))
	assert.True(t, containsTimestamp("18:31:42-03:30"))
	assert.True(t, containsTimestamp("2005-08-09T18:31:42"))
	assert.True(t, containsTimestamp("2005-08-09T18:31:42+03"))
	assert.True(t, containsTimestamp("2005-08-09T18:31:42-03"))
	assert.True(t, containsTimestamp("2005-08-09T18:31:42+03:30"))
	assert.True(t, containsTimestamp("2005-08-09T18:31:42-03:30"))
	assert.True(t, containsTimestamp("2005-08-09T18:31:42"))
	assert.True(t, containsTimestamp("2005-08-09T18:31:42.201"))
	assert.True(t, containsTimestamp(`10/Oct/2000:13:55:36 -0700`))
	assert.True(t, containsTimestamp(time.ANSIC))
	assert.True(t, containsTimestamp(time.UnixDate))
	assert.True(t, containsTimestamp(time.RubyDate))
	assert.True(t, containsTimestamp(time.RFC850))
	assert.True(t, containsTimestamp(time.RFC1123))
	assert.True(t, containsTimestamp(time.RFC1123Z))
	assert.True(t, containsTimestamp(time.RFC3339))
	assert.True(t, containsTimestamp(time.RFC3339Nano))
	assert.True(t, containsTimestamp(time.Stamp))
	assert.True(t, containsTimestamp(time.StampMilli))
	assert.True(t, containsTimestamp(time.StampMicro))

	assert.False(t, containsTimestamp("13/32"))
	assert.False(t, containsTimestamp("13:32"))
	assert.False(t, containsTimestamp("100/5/100"))
	assert.False(t, containsTimestamp("1:12:123"))
	assert.False(t, containsTimestamp("12:aa:12:32"))

}

func Benchmark_containsTimestamp(b *testing.B) {
	l := `10.42.0.21 - - [30/Oct/2023:11:55:47 +0000] "GET / HTTP/1.1" 200 612 "-" "-" "-"`
	for n := 0; n < b.N; n++ {
		containsTimestamp(l)
	}
}
