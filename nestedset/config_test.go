package nestedset

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestConfigDefaults(t *testing.T) {
	t.Setenv(EnabledEnv, "true")
	got, err := ConfigFromEnv(noWarn(t))
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
	got, err := ConfigFromEnv(noWarn(t))
	want := Config{Enabled: true, Idle: time.Second, Interval: 250 * time.Millisecond, MaxSpans: 500}
	if err != nil || got != want {
		t.Errorf("got %+v, %v; want %+v", got, err, want)
	}
}

func TestConfigIsOffByDefault(t *testing.T) {
	got, err := ConfigFromEnv(noWarn(t))
	if err != nil || got.Enabled {
		t.Errorf("got %+v, %v; want disabled", got, err)
	}
}

func TestConfigRejectsInvalidValuesOnlyWhenEnabled(t *testing.T) {
	for _, c := range []struct{ env, val string }{
		{IdleEnv, "10"},
		{IdleEnv, "0s"},
		{IntervalEnv, "-1s"},
		{IntervalEnv, "soon"},
		{MaxSpansEnv, "0"},
		{MaxSpansEnv, "lots"},
	} {
		t.Run(c.env+"="+c.val, func(t *testing.T) {
			t.Setenv(c.env, c.val)
			if _, err := ConfigFromEnv(noWarn(t)); err != nil {
				t.Errorf("disabled: %v", err)
			}
			t.Setenv(EnabledEnv, "true")
			if _, err := ConfigFromEnv(noWarn(t)); err == nil || !strings.Contains(err.Error(), c.env) {
				t.Errorf("got %v, want an error naming %s", err, c.env)
			}
		})
	}
}

func TestConfigTreatsAnUnrecognisedEnabledValueAsDisabled(t *testing.T) {
	t.Setenv(EnabledEnv, " Maybe")
	t.Setenv(IdleEnv, "soon")
	var warnings []string
	got, err := ConfigFromEnv(func(args ...any) { warnings = append(warnings, fmt.Sprint(args...)) })
	if err != nil || got.Enabled {
		t.Errorf("got %+v, %v; want disabled without an error", got, err)
	}
	if len(warnings) != 1 || !strings.Contains(warnings[0], EnabledEnv) || !strings.Contains(warnings[0], `" Maybe"`) {
		t.Errorf("got warnings %q, want one naming %s and the value", warnings, EnabledEnv)
	}
}

func noWarn(t *testing.T) func(...any) {
	return func(args ...any) { t.Errorf("unexpected warning: %s", fmt.Sprint(args...)) }
}
