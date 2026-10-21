package nestedset

import (
	"strings"
	"testing"
	"time"
)

func TestConfigDefaults(t *testing.T) {
	t.Setenv(EnabledEnv, "true")
	got, err := ConfigFromEnv()
	want := Config{Enabled: true, Idle: 10 * time.Second, Interval: 5 * time.Second, MaxSpans: 200000}
	if err != nil || got != want {
		t.Errorf("got %+v, %v; want %+v", got, err, want)
	}
}

func TestConfigReadsTheSettings(t *testing.T) {
	t.Setenv(EnabledEnv, "1")
	t.Setenv(IdleEnv, "1s")
	t.Setenv(IntervalEnv, "250ms")
	t.Setenv(MaxSpansEnv, "500")
	got, err := ConfigFromEnv()
	want := Config{Enabled: true, Idle: time.Second, Interval: 250 * time.Millisecond, MaxSpans: 500}
	if err != nil || got != want {
		t.Errorf("got %+v, %v; want %+v", got, err, want)
	}
}

func TestConfigIsOffByDefault(t *testing.T) {
	got, err := ConfigFromEnv()
	if err != nil || got.Enabled {
		t.Errorf("got %+v, %v; want disabled", got, err)
	}
}

func TestConfigRejectsInvalidValuesOnlyWhenEnabled(t *testing.T) {
	for _, c := range []struct{ env, val string }{
		{EnabledEnv, "maybe"},
		{IdleEnv, "10"},
		{IdleEnv, "0s"},
		{IntervalEnv, "-1s"},
		{IntervalEnv, "soon"},
		{MaxSpansEnv, "0"},
		{MaxSpansEnv, "lots"},
	} {
		t.Run(c.env+"="+c.val, func(t *testing.T) {
			t.Setenv(c.env, c.val)
			if c.env != EnabledEnv {
				if _, err := ConfigFromEnv(); err != nil {
					t.Errorf("disabled: %v", err)
				}
				t.Setenv(EnabledEnv, "true")
			}
			if _, err := ConfigFromEnv(); err == nil || !strings.Contains(err.Error(), c.env) {
				t.Errorf("got %v, want an error naming %s", err, c.env)
			}
		})
	}
}
