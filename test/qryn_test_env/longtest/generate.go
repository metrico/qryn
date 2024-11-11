package main

import (
	"bytes"
	"fmt"
	"github.com/akvlad/flog/generator"
	"github.com/scaleway/scaleway-sdk-go/namegenerator"
	"strconv"
	"sync"
	"time"
)

type bufCloser struct {
	*bytes.Buffer
}

func (*bufCloser) Close() error {
	return nil
}

func generateLogs() []string {
	var res []string
	writers := make([]bytes.Buffer, 8)
	wg := sync.WaitGroup{}
	for i, format := range []string{"apache_common", "apache_combined", "apache_error", "rfc3164", "rfc5424",
		"common_log", "json"} {
		wg.Add(1)
		go func(format string, i int) {
			defer wg.Done()
			generator.Generate(&generator.Option{
				Format:    format,
				Output:    "",
				Type:      "stdout",
				Number:    0,
				Bytes:     10 * 1024 * 1024,
				Sleep:     0,
				Delay:     0,
				SplitBy:   0,
				Overwrite: false,
				Forever:   false,
				Writer:    &bufCloser{&writers[i]},
			})
		}(format, i)
	}
	generateFaro(&writers[7])
	wg.Wait()
	for _, w := range writers {
		lines := bytes.Split(w.Bytes(), []byte("\n"))
		for _, l := range lines {
			res = append(res, string(l))
		}
	}
	return res
}

func generateFaro(buf *bytes.Buffer) {
	_buf := bytes.Buffer{}

	generator.Generate(&generator.Option{
		Format:    "common_log",
		Output:    "",
		Type:      "stdout",
		Number:    0,
		Bytes:     10 * 1024 * 1024,
		Sleep:     0,
		Delay:     0,
		SplitBy:   0,
		Overwrite: false,
		Forever:   false,
		Writer:    &bufCloser{&_buf},
	})

	lines := bytes.Split(_buf.Bytes(), []byte("\n"))
	for _, l := range lines {
		buf.WriteString(fmt.Sprintf(
			"timestamp=\"%s\" kind=log message=%s level=log sdk_name=@grafana/faro-core sdk_version=1.0.0 sdk_integrations=@grafana/faro-web-sdk:instrumentation-errors:1.0.0,@grafana/faro-web-sdk:instrumentation-web-vitals:1.0.0,@grafana/faro-web-sdk:instrumentation-session:1.0.32,@grafana/faro-web-sdk:instrumentation-view:1.0.32,@grafana/faro-web-sdk:instrumentation-console:1.0.0,@grafana/faro-web-tracing:1.0.0,@grafana/faro-react:1.0.0 app_name=@grafana/faro-demo-client app_version=1.0.0 app_environment=production session_id=fDKz3Gccz6 page_url=http://localhost:5173/ browser_name=Firefox browser_version=122.0 browser_os=\"Ubuntu unknown\" browser_mobile=false view_name=default\n",
			time.Now().UTC().Format(time.RFC3339Nano), strconv.Quote(string(l))))
	}
}

func generateNames(n int) []string {
	names := make([]string, n)
	for i := range names {
		names[i] = namegenerator.GetRandomName()
	}
	return names
}
