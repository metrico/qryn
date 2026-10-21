package nestedset

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	// EnabledEnv runs the numberer. Enable it on exactly one instance.
	EnabledEnv = "GIGAPIPE_NESTED_SET_NUMBERER_ENABLED"
	// IdleEnv is how long a trace receives no spans before it is numbered.
	IdleEnv = "GIGAPIPE_NESTED_SET_NUMBERER_IDLE"
	// IntervalEnv is the time between passes.
	IntervalEnv = "GIGAPIPE_NESTED_SET_NUMBERER_INTERVAL"
	// MaxSpansEnv caps the spans one pass numbers.
	MaxSpansEnv = "GIGAPIPE_NESTED_SET_NUMBERER_MAX_SPANS"
)

// Config holds the numberer settings.
type Config struct {
	Enabled        bool
	Idle, Interval time.Duration
	MaxSpans       int
}

// ConfigFromEnv reads the settings. An unrecognised EnabledEnv value is a
// warning and leaves the numberer disabled; the other values are validated
// only when it is enabled.
func ConfigFromEnv(warn func(args ...any)) (Config, error) {
	cfg := Config{Idle: 10 * time.Second, Interval: 5 * time.Second, MaxSpans: 200000}
	switch v := strings.ToLower(strings.TrimSpace(os.Getenv(EnabledEnv))); v {
	case "1", "true", "yes", "on":
		cfg.Enabled = true
	case "", "0", "false", "no", "off":
		return cfg, nil
	default:
		warn(fmt.Sprintf("%s=%q: want true or false; nested-set numberer disabled", EnabledEnv, os.Getenv(EnabledEnv)))
		return cfg, nil
	}
	for _, d := range []struct {
		env string
		to  *time.Duration
	}{{IdleEnv, &cfg.Idle}, {IntervalEnv, &cfg.Interval}} {
		v := os.Getenv(d.env)
		if v == "" {
			continue
		}
		parsed, err := time.ParseDuration(v)
		if err != nil || parsed <= 0 {
			return cfg, fmt.Errorf("%s=%q: want a positive Go duration such as 10s", d.env, v)
		}
		*d.to = parsed
	}
	if v := os.Getenv(MaxSpansEnv); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			return cfg, fmt.Errorf("%s=%q: want a positive integer", MaxSpansEnv, v)
		}
		cfg.MaxSpans = n
	}
	return cfg, nil
}
