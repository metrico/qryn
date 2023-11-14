package logparser

import (
	"strings"
	"unicode"
)

type Level int

const (
	LevelUnknown Level = iota
	LevelCritical
	LevelError
	LevelWarning
	LevelInfo
	LevelDebug

	maxLineLenForGuessingLevel = 255
	guessLevelInFields         = 7
)

func (l Level) String() string {
	switch l {
	case LevelCritical:
		return "critical"
	case LevelError:
		return "error"
	case LevelWarning:
		return "warning"
	case LevelInfo:
		return "info"
	case LevelDebug:
		return "debug"
	}
	return "unknown"
}

var (
	glogLevelsMapping = map[byte]Level{
		'I': LevelInfo,
		'W': LevelWarning,
		'E': LevelError,
		'F': LevelCritical,
	}
	priority2Levels = map[string]Level{
		"0": LevelCritical,
		"1": LevelCritical,
		"2": LevelCritical,
		"3": LevelError,
		"4": LevelWarning,
		"5": LevelInfo,
		"6": LevelInfo,
		"7": LevelDebug,
	}
)

func LevelByPriority(priority string) Level {
	if level, ok := priority2Levels[priority]; ok {
		return level
	}
	return LevelUnknown
}

func GuessLevel(line string) Level {
	if len(line) > maxLineLenForGuessingLevel {
		line = line[:maxLineLenForGuessingLevel]
	}
	fields := strings.Fields(line)
	if len(fields) == 0 {
		return LevelUnknown
	}
	limit := len(fields)
	if limit > guessLevelInFields {
		limit = guessLevelInFields
	}

	if l := tryGlog(fields); l != LevelUnknown {
		return l
	}

	for _, f := range fields[:limit] {
		subfields := strings.FieldsFunc(f, func(r rune) bool {
			return r == ']' || r == ')' || r == ';' || r == '|' || r == ':' || r == ',' || r == '.'
		})
		for _, sf := range subfields {
			sf = strings.TrimLeft(strings.ToLower(sf), "\"[(<'")
			sf = strings.TrimPrefix(sf, "Level=")
			if len(sf) < 4 {
				continue
			}
			switch sf[:4] {
			case "debu":
				return LevelDebug
			case "info", "noti":
				return LevelInfo
			case "warn":
				return LevelWarning
			case "erro":
				return LevelError
			case "crit", "fata":
				return LevelCritical
			}
		}
	}
	if l := guessRedisLevel(fields); l != LevelUnknown {
		return l
	}
	return LevelUnknown
}

func tryGlog(fields []string) Level {
	firstField := fields[0]
	if len(firstField) != 5 {
		return LevelUnknown
	}
	level, ok := glogLevelsMapping[firstField[0]]
	if !ok {
		return LevelUnknown
	}
	for _, r := range firstField[1:] {
		if !unicode.IsDigit(r) {
			return LevelUnknown
		}
	}
	return level
}

// redis 2.x
// [pid] date loglevel message
// [4018] 14 Nov 07:01:22.119 * Background saving terminated with success

// redis 3.x+
// pid:role timestamp loglevel message
// 1:S 12 Nov 07:52:11.999 * FAIL message received from X about Y

// redis 5.x: the year was added
// 1:S 12 Nov 2019 07:52:11.999 * FAIL message received from X about Y
func guessRedisLevel(fields []string) Level {
	if len(fields) < 6 {
		return LevelUnknown
	}
	if strings.HasPrefix(fields[0], "[") && strings.HasSuffix(fields[0], "]") {
		return redisCharToLevel(fields[4])
	}
	if len(strings.Split(fields[0], ":")) == 2 {
		if len(fields[3]) == 4 { //redis 5.x+
			return redisCharToLevel(fields[5])
		} else {
			return redisCharToLevel(fields[4])
		}
	}
	return LevelUnknown
}

func redisCharToLevel(level string) Level {
	switch level {
	case ".":
		return LevelDebug
	case "-":
		return LevelInfo
	case "*", "#":
		return LevelWarning
	}
	return LevelUnknown
}

// todo
//
// python
//
//   loglevels: DEBUG, INFO, WARNING, ERROR,CRITICAL
// pylogging:
//  default "%(levelname)s:%(name)s:%(message)s" https://github.com/python/cpython/blob/master/Lib/logging/__init__.py#L502
// django:
//   {levelname} {message}
//   {asctime} {module} [{levelname}] okserver
//   [%(asctime)s] %(levelname)s
//   %(levelname)s %(asctime)s %(module)s: %(message)s
// asctime: %(asctime)s Human-readable time when the LogRecord was created. By default this is of the form ‘2003-07-08 16:49:45,896’ (the numbers after the comma are millisecond portion of the time).
//
