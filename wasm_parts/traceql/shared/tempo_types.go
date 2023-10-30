package shared

type TraceInfo struct {
	TraceID           string  `json:"traceID"`
	RootServiceName   string  `json:"rootServiceName"`
	RootTraceName     string  `json:"rootTraceName"`
	StartTimeUnixNano string  `json:"startTimeUnixNano"`
	DurationMs        float64 `json:"durationMs"`
	SpanSet           SpanSet `json:"spanSet"`
}

type SpanInfo struct {
	SpanID            string     `json:"spanID"`
	StartTimeUnixNano string     `json:"startTimeUnixNano"`
	DurationNanos     string     `json:"durationNanos"`
	Attributes        []SpanAttr `json:"attributes"`
}

type SpanSet struct {
	Spans   []SpanInfo `json:"spans"`
	Matched int        `json:"matched"`
}

type SpanAttr struct {
	Key   string `json:"key"`
	Value struct {
		StringValue string `json:"stringValue"`
	} `json:"value"`
}

type TraceRequestProcessor interface {
	Process(*PlannerContext) (chan []TraceInfo, error)
}
