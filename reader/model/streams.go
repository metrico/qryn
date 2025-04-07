package model

import "time"

type PushRequest struct {
	Streams []Stream `json:"streams"`
}
type Stream struct {
	Labels  string  `json:"labels"`
	Entries []Entry `json:"entries"`
}

// Entry is a log entry with a timestamp.
type Entry struct {
	Timestamp time.Time `json:"timestamp"`
	Line      string    `json:"line"`
}

type Label struct {
	Key, Value string
}

type LabelRules struct {
	Label, Cond, Value string
}
