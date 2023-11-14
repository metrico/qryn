package logparser

import (
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

var (
	multilineCollectorTimeout = time.Millisecond * 100
)

type Message struct {
	Timestamp time.Time
	Content   string
	Level     Level
}

type MultilineCollector struct {
	OnNewMessage                 func(msg Message)
	ts                           time.Time
	level                        Level
	lines                        []string
	isFirstLineContainsTimestamp bool
	timeout                      time.Duration
	lastReceiveTime              time.Time
	closed                       bool
	lock                         sync.Mutex
}

func NewMultilineCollector(timeout time.Duration, onNewMessage func(m Message)) *MultilineCollector {
	m := &MultilineCollector{
		timeout:      timeout,
		OnNewMessage: onNewMessage,
	}
	return m
}

func (m *MultilineCollector) Add(entry LogEntry) {
	if !utf8.ValidString(entry.Content) {
		return
	}

	m.lock.Lock()
	defer m.lock.Unlock()

	if time.Now().Sub(m.lastReceiveTime) > m.timeout {
		m.flushMessage()
	}

	entry.Content = strings.TrimSuffix(entry.Content, "\n")
	if entry.Content == "" {
		if len(m.lines) > 0 {
			m.add(entry)
		}
		return
	}
	if !m.isPrevMsgPart(entry.Content) {
		m.flushMessage()
	}
	m.add(entry)
}

func (m *MultilineCollector) add(entry LogEntry) {
	if len(m.lines) == 0 {
		m.ts = entry.Timestamp
		m.level = GuessLevel(entry.Content)
		if m.level == LevelUnknown && entry.Level != LevelUnknown {
			m.level = entry.Level
		}
		m.isFirstLineContainsTimestamp = containsTimestamp(entry.Content)
	}
	m.appendLine(entry.Content)
	m.lastReceiveTime = time.Now()
}

func (m *MultilineCollector) appendLine(value string) {
	m.lines = append(m.lines, value)
}

func (m *MultilineCollector) isPrevMsgPart(l string) bool {
	if len(m.lines) == 0 {
		return true
	}

	if m.isFirstLineContainsTimestamp {
		if !containsTimestamp(l) {
			return true
		}
		return false
	}

	if strings.HasPrefix(l, "\tat ") || strings.HasPrefix(l, "\t... ") {
		return true
	}

	if strings.HasPrefix(l, "Traceback (") || strings.HasPrefix(l, "  File") || strings.HasPrefix(l, "    ") {
		return true
	}

	if strings.HasPrefix(l, "Caused by: ") {
		return true
	}
	if l == "The above exception was the direct cause of the following exception:" || l == "During handling of the above exception, another exception occurred:" {
		return true
	}
	prevLine := m.lines[len(m.lines)-1]
	if strings.HasPrefix(prevLine, "    ") || strings.HasPrefix(prevLine, "  File") || strings.HasSuffix(prevLine, "with root cause") {
		return true
	}
	return false
}

func (m *MultilineCollector) flushMessage() {
	if m.closed {
		return
	}
	if len(m.lines) == 0 {
		return
	}
	content := strings.TrimSpace(strings.Join(m.lines, "\n"))
	msg := Message{
		Timestamp: m.ts,
		Content:   content,
		Level:     m.level,
	}
	m.reset()
	if m.OnNewMessage != nil {
		m.OnNewMessage(msg)
	}
}

func (m *MultilineCollector) reset() {
	m.ts = time.Time{}
	m.lines = m.lines[:0]
	m.isFirstLineContainsTimestamp = false
	m.level = LevelUnknown
}
