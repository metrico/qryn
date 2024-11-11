package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

func main() {
	kind := os.Getenv("KIND")
	if kind == "" {
		kind = "WRITE"
	}
	switch kind {
	case "WRITE":
		writeTest()
		break
	case "READ":
		readTest()
		break
	}
}

func writeTest() {
	fmt.Println("GENERATING")
	logs := generateLogs()
	//names := generateNames(1500)
	fmt.Println("SENDING")
	names := generateNames(3300)
	power := 1
	if os.Getenv("POWER") != "" {
		var err error
		power, err = strconv.Atoi(os.Getenv("POWER"))
		if err != nil {
			panic(err)
		}
	}
	headers := map[string]string{}
	if strings.Contains(os.Getenv("MODE"), "L") {
		fmt.Println("Run logs")
		sender := NewLogSender(LogSenderOpts{
			ID:         "logs",
			Containers: names,
			Lines:      logs,
			LinesPS:    120 * power,
			URL:        os.Getenv("URL"),
			Headers:    headers,
		})
		sender.Run()
	}
	/*if strings.Contains(os.Getenv("MODE"), "P") {
	        fmt.Println("Run logs PB")
	        _headers := make(map[string]string, 20)
	        for k, v := range headers {
	                _headers[k] = v
	        }
	        _headers["Content-Type"] = "application/x-protobuf"
	        sender := NewPBSender(LogSenderOpts{
	                ID:         "logs",
	                Containers: names,
	                Lines:      logs,
	                LinesPS:    50000,
	                URL:        os.Getenv("URL"),
	                Headers:    _headers,
	        })
	        sender.Run()
	}*/
	if strings.Contains(os.Getenv("MODE"), "M") {
		fmt.Println("Run metrics")
		metrics := NewMetricSender(LogSenderOpts{
			ID:         "metrics",
			Containers: names,
			Lines:      logs,
			LinesPS:    30 * power,
			URL:        os.Getenv("URL"),
			Headers:    headers,
		})
		metrics.Run()
	}
	if strings.Contains(os.Getenv("MODE"), "Z") {
		fmt.Println("Run zipkin")
		zipkins := NewZipkinSender(LogSenderOpts{
			ID:         "traces",
			Containers: names,
			Lines:      logs,
			LinesPS:    40 * power,
			URL:        os.Getenv("URL"),
			Headers:    headers,
		})
		zipkins.Run()
	}
	if strings.Contains(os.Getenv("MODE"), "O") {
		fmt.Println("Run OTLP")
		zipkins := NewOTLPSender(LogSenderOpts{
			ID:         "traces",
			Containers: names,
			Lines:      logs,
			LinesPS:    40 * power,
			URL:        os.Getenv("URL"),
			Headers:    headers,
		})
		zipkins.Run()
	}
	if strings.Contains(os.Getenv("MODE"), "G") {
		fmt.Println("Run zipkin")
		zipkins := NewSGSender(LogSenderOpts{
			ID:         "traces",
			Containers: names,
			Lines:      logs,
			LinesPS:    10 * power,
			URL:        os.Getenv("URL"),
			Headers:    headers,
		})
		zipkins.Run()
	}
	if strings.Contains(os.Getenv("MODE"), "D") {
		fmt.Println("Run datadog")
		datadogs := NewDatadogSender(LogSenderOpts{
			ID:         "traces",
			Containers: names,
			Lines:      logs,
			LinesPS:    120 * power,
			URL:        os.Getenv("URL"),
			Headers:    headers,
		})
		datadogs.Run()
	}
	if strings.Contains(os.Getenv("MODE"), "I") {
		fmt.Println("Run influx")
		influx := NewInfluxSender(LogSenderOpts{
			ID:         "influx",
			Containers: names,
			Lines:      logs,
			LinesPS:    100 * power,
			URL:        os.Getenv("URL"),
			Headers:    headers,
		})
		influx.Run()
	}
	if strings.Contains(os.Getenv("MODE"), "C") {
		fmt.Println("Run consistency checker")
		cons := NewJSONConsistencyChecker(LogSenderOpts{
			ID:         "consistency-1",
			Containers: names,
			Lines:      logs,
			LinesPS:    300 * power,
			URL:        os.Getenv("URL"),
			Headers:    headers,
		})
		cons.Run()
	}
	if strings.Contains(os.Getenv("MODE"), "T") {
		fmt.Println("Run time sender")
		pqt := NewTimeSender(LogSenderOpts{
			ID:         "longtest-TIME",
			Containers: names,
			Lines:      logs,
			LinesPS:    10,
			URL:        os.Getenv("URL"),
			Headers:    headers,
		})
		pqt.Run()
	}
	t := time.NewTicker(time.Second)
	go func() {
		for range t.C {
			s := stats.Collect()
			fmt.Printf("Ok requests: %d, Errors: %d, Failed: %d\n", s[REQ_OK], s[REQ_ERR], s[REQ_FAIL])
			fmt.Printf("Ok Requests time: min: %d, max: %d, avg: %f\n",
				s[REQ_TIME_MS+"_min"],
				s[REQ_TIME_MS+"_max"],
				float64(s[REQ_TIME_MS+"_sum"])/float64(s[REQ_TIME_MS+"_count"]))
			fmt.Printf("Ok Requests MB sent: %f, (%fMB/s)\n",
				float64(s[REQ_BYTES+"_sum"])/1024/1024,
				float64(s[REQ_BYTES+"_sum"])/1024/1024/5,
			)
		}
	}()
	for {
		time.Sleep(time.Second)
	}
}

func readTest() {

}
