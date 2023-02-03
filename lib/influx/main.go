package main

import (
	"fmt"
	telegraf "github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/parsers/influx"
	"strconv"
	"strings"
	"time"
)

func main() {}

var buff []byte
var err error
var metrics []telegraf.Metric
var resp []byte

//export CreateBuff
func CreateBuff(len int32) *byte {
	buff = make([]byte, len)
	return &buff[0]
}

//export ParseBytes
func ParseBytes() {
	hndl := influx.NewMetricHandler()
	hndl.SetTimePrecision(time.Nanosecond)
	parser := influx.NewParser(hndl)
	metrics, err = parser.Parse(buff)
	if err != nil {
		resp = []byte(fmt.Sprintf(`{"error": %s}`, strconv.Quote(err.Error())))
		return
	}
	var mapRes []string
	for _, m := range metrics {
		var tags []string
		for _, t := range m.TagList() {
			tags = append(tags, fmt.Sprintf("%s:%s",
				strconv.Quote(t.Key),
				strconv.Quote(t.Value)))
		}
		var fields []string
		for k, f := range m.Fields() {
			var strField string
			switch f.(type) {
			case int64:
				strField = strconv.FormatInt(f.(int64), 10)
			case float64:
				strField = strconv.FormatFloat(f.(float64), 'f', 10, 64)
			case string:
				strField = strconv.Quote(f.(string))
			case bool:
				strField = strconv.FormatBool(f.(bool))
			}
			if strField != "" {
				fields = append(fields, fmt.Sprintf("%s:%s", strconv.Quote(k), strField))
			}
		}
		ent := fmt.Sprintf(`{"timestamp":"%d","measurement": %s, "tags": {%s}, "fields": {%s}}`,
			m.Time().UnixNano(),
			strconv.Quote(m.Name()),
			strings.Join(tags, ","),
			strings.Join(fields, ","))
		mapRes = append(mapRes, ent)
	}
	resp = []byte(fmt.Sprintf("[%s]", strings.Join(mapRes, ",")))
}

//export GetLen
func GetLen() int32 {
	return int32(len(resp))
}

//export GetResp
func GetResp() *byte {
	return &resp[0]
}

//export Free
func Free() {
	buff = nil
	err = nil
	metrics = nil
	resp = nil
}
