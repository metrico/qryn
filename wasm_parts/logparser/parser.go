package logparser

import (
	"sync"
	"time"
)

type LogEntry struct {
	Timestamp time.Time
	Content   string
	Level     Level
}

type LogCounter struct {
	Level    Level
	Hash     string
	Sample   string
	Messages int
}

type Parser struct {
	decoder Decoder

	patterns map[PatternKey]*PatternStat
	lock     sync.RWMutex

	multilineCollector *MultilineCollector

	onMsgCb OnMsgCallbackF
}

type OnMsgCallbackF func(ts time.Time, level Level, patternHash string, msg string)

func NewParser(decoder Decoder, onMsgCallback OnMsgCallbackF) *Parser {
	p := &Parser{
		decoder:  decoder,
		patterns: map[PatternKey]*PatternStat{},
		onMsgCb:  onMsgCallback,
	}
	p.multilineCollector = NewMultilineCollector(multilineCollectorTimeout, p.inc)
	return p
}

func (p *Parser) NewLogEntry(entry LogEntry) {
	var err error
	if p.decoder != nil {
		if entry.Content, err = p.decoder.Decode(entry.Content); err != nil {
			return
		}
	}
	p.multilineCollector.Add(entry)
}

func (p *Parser) inc(msg Message) {
	p.lock.Lock()
	defer p.lock.Unlock()

	if msg.Level == LevelUnknown || msg.Level == LevelDebug || msg.Level == LevelInfo {
		key := PatternKey{Level: msg.Level, Hash: ""}
		if stat := p.patterns[key]; stat == nil {
			p.patterns[key] = &PatternStat{}
		}
		p.patterns[key].Messages++
		if p.onMsgCb != nil {
			p.onMsgCb(msg.Timestamp, msg.Level, "", msg.Content)
		}
		return
	}

	pattern := NewPattern(msg.Content)
	key := PatternKey{Level: msg.Level, Hash: pattern.Hash()}
	stat := p.patterns[key]
	if stat == nil {
		for k, ps := range p.patterns {
			if k.Level == msg.Level && ps.Pattern.WeakEqual(pattern) {
				stat = ps
				break
			}
		}
		if stat == nil {
			stat = &PatternStat{Pattern: pattern, Sample: msg.Content}
			p.patterns[key] = stat
		}
	}
	if p.onMsgCb != nil {
		p.onMsgCb(msg.Timestamp, msg.Level, key.Hash, msg.Content)
	}
	stat.Messages++
}

func (p *Parser) Merge(key PatternKey, stat PatternStat) {
	p.lock.Lock()
	defer p.lock.Unlock()

	oldStat := p.patterns[key]
	if oldStat == nil {
		for k, ps := range p.patterns {
			if k.Level == key.Level && ps.Pattern.WeakEqual(stat.Pattern) {
				oldStat = ps
				break
			}
		}
	}

	if oldStat == nil {
		p.patterns[key] = &stat
		return
	}

	oldStat.Messages += stat.Messages
}

func (p *Parser) GetCounters() []LogCounter {
	p.lock.RLock()
	defer p.lock.RUnlock()
	res := make([]LogCounter, 0, len(p.patterns))
	for k, ps := range p.patterns {
		res = append(res, LogCounter{Level: k.Level, Hash: k.Hash, Sample: ps.Sample, Messages: ps.Messages})
	}
	return res
}

func (p *Parser) GetPatterns() map[PatternKey]*PatternStat {
	return p.patterns
}

type PatternKey struct {
	Level Level
	Hash  string
}

type PatternStat struct {
	Pattern  *Pattern
	Sample   string
	Messages int
}
