package logparser

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestPattern(t *testing.T) {
	assert.Equal(t,
		"package.name got things in",
		NewPattern("2019-07-24 12:06:21,688 package.name [DEBUG] got 10 things in 3.1s").String())

	assert.Equal(t,
		"INFO GET",
		NewPattern("INFO 192.168.1.6 GET /standalone?job_cycles=50000&sleep=20ms&sleep_jitter_percent=500 200 0.113s").String())

	assert.Equal(t,
		"WARN client closed connection after",
		NewPattern("WARN client 192.168.1.8:57600 closed connection after 1.000s").String())

	assert.Equal(t,
		"Jun host kubelet watch of ended with too old resource version",
		NewPattern("Jun 16 21:41:24 host01 kubelet[961]: W0616 21:41:24.642736     961 reflector.go:341] k8s.io/kubernetes/pkg/kubelet/config/apiserver.go:47: watch of *v1.Pod ended with: too old resource version: 81608152 (81608817)").String())

	assert.Equal(t,
		"Unable to ensure the docker processes run in the desired containers errors moving pid failed to find pid namespace of process",
		NewPattern(`Unable to ensure the docker processes run in the desired containers: errors moving "docker-containerd" pid: failed to find pid namespace of process '㌟'`).String())

	assert.Equal(t,
		"ExecSync from runtime service failed rpc error code Unknown desc container not running",
		NewPattern("ExecSync 099a0cbb70555d5d0e1823993175947487c9bc075171df5a161d8e46456b232c 'bash -c echo -ne \x01\x04\x00\x00\x00\x00 | nc 127.0.0.1 81' from runtime service failed: rpc error: code = Unknown desc = container not running (099a0cbb70555d5d0e1823993175947487c9bc075171df5a161d8e46456b232c)").String())

	assert.Equal(t,
		"Get request canceled",
		NewPattern("2019/07/23 15:21:08 http-load-generator.go:49: Get http://golang-app/standalone?job_cycles=50000\u0026sleep=20ms\u0026sleep_jitter_percent=500: net/http: request canceled (Client.Timeout exceeded while awaiting headers)").String())

	assert.Equal(t,
		"query for app done in",
		NewPattern(`2019/07/24 10:40:38.887696 module.go:3334: [INFO: 3fe862d0-f5d0-460f-88d5-e6088985e881]: query "{app!=[xz,xz3],name=[long.name]}" for app="xzxzx" done in 0.016s`).String())

	assert.Equal(t,
		"",
		NewPattern(`[Full GC (Allocation Failure) [CMS: 176934K->176934K(176960K), 0.0451364 secs] 253546K->253546K(253632K), [Metaspace: 11797K->11797K(1060864K)], 0.0454767 secs] [Times: user=0.04 sys=0.00, real=0.05 secs]`).String())

	assert.Equal(t,
		"Nov FAIL message received from about",
		NewPattern(`1:S 12 Nov 2019 07:52:11.999 * FAIL message received from b9112fbdd53291f1924bd3ff81d24b4d48e38929 about e16a51c1d8639a6cc904d8c4dce4ef6d5a1287c3`).String())

	assert.Equal(t,
		"Nov Start of election delayed for milliseconds",
		NewPattern(`1:S 12 Nov 2019 13:17:07.347 # Start of election delayed for 502 milliseconds (rank #0, offset 99524)`).String())

	assert.Equal(t,
		"WARN org.eclipse.jetty.server.HttpChannel",
		NewPattern(`11227 [qtp672320506-43] WARN org.eclipse.jetty.server.HttpChannel  - /`).String())

	assert.Equal(t,
		NewPattern("\tat sun.reflect.GeneratedMethodAccessor72.invoke(Unknown Source) ~[na:na]").String(),
		NewPattern("\tat sun.reflect.GeneratedMethodAccessor71.invoke(Unknown Source) ~[na:na]").String())

	assert.Equal(t,
		NewPattern("ERROR 1 --- [io-8080-exec-18] o.h.engine.jdbc.spi.SqlExceptionHelper : Too many connections").String(),
		NewPattern("ERROR 1 --- [nio-8080-exec-9] o.h.engine.jdbc.spi.SqlExceptionHelper : Too many connections").String())

	assert.Equal(t,
		"no results match selector",
		NewPattern(`[WARNING] no results match selector: {'status': ['1*', '2'], 'app': 'app1', 'host': 'parse*'}"}`).String())

	assert.Equal(t,
		"WARNING items are not found for project UniqueName",
		NewPattern(`WARNING: d2cf9441-82d6-4fc6-8c16-d2a8531ff4a5 26 items are not found {name=[aaaabbbbbcccc]} for project UniqueName`).String())

	assert.Equal(t,
		"Dec gke-foo---bcbd-node-eoj startupscript Finished running startup script",
		NewPattern(`Dec 21 23:17:22 gke-foo-1-1-4b5cbd14-node-4eoj startupscript: Finished running startup script /var/run/google.startup.script`).String())
}

func TestPatternWeakEqual(t *testing.T) {
	assert.True(t, NewPattern("foo one baz").WeakEqual(NewPattern("foo two baz")))
	assert.True(t, NewPattern("foo baz one").WeakEqual(NewPattern("foo baz two")))
	assert.False(t, NewPattern("foo bar baz").WeakEqual(NewPattern("foo barr bazz")))
	assert.False(t, NewPattern("foo bar baz").WeakEqual(NewPattern("baz bar foo")))
}

func TestPatternRemoveQuotedAndBrackets(t *testing.T) {
	assert.Equal(t, "foo  bar", removeQuotedAndBrackets(`foo 'squoted' bar`))
	assert.Equal(t, "foo  bar", removeQuotedAndBrackets(`foo 'squoted \'baz\'' bar`))
	assert.Equal(t, "foo  bar", removeQuotedAndBrackets(`foo "dquoted" bar`))
	assert.Equal(t, "foo  bar", removeQuotedAndBrackets(`foo "dquoted \"baz\"" bar`))
	assert.Equal(t, "foo  bar", removeQuotedAndBrackets(`foo "dquoted 'squoted' " bar`))
	assert.Equal(t, "foo  bar", removeQuotedAndBrackets(`foo 'squoted "baz"' bar`))

	assert.Equal(t, " msg", removeQuotedAndBrackets(`[nio-8080-exec-9] msg`))
	assert.Equal(t, "json: ", removeQuotedAndBrackets(`json: {'arr': ['1', '2'], 'str': 'strval', 'age': 20}`))
	assert.Equal(t, " ",
		removeQuotedAndBrackets(`[Full GC (Allocation Failure) [CMS: 176934K->176934K(176960K), 0.0451364 secs] 253546K->253546K(253632K), [Metaspace: 11797K->11797K(1060864K)], 0.0454767 secs] [Times: user=0.04 sys=0.00, real=0.05 secs]`))
	assert.Equal(t,
		"Jun 16 21:41:24 host01 kubelet: W0616 21:41:24.642736     961 reflector.go:341]",
		removeQuotedAndBrackets(`Jun 16 21:41:24 host01 kubelet[961]: W0616 21:41:24.642736     961 reflector.go:341]`))
}
