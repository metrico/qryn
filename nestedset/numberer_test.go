package nestedset

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestSupportedVersion(t *testing.T) {
	for v, want := range map[string]bool{
		"24.9.3.128": true, "24.10.1": true, "25.3.14.14": true, "100.1": true,
		"24.8.14.39": false, "23.12.1": false, "24": false, "": false, "x.y": false,
	} {
		if got := SupportedVersion(v); got != want {
			t.Errorf("SupportedVersion(%q) = %v, want %v", v, got, want)
		}
	}
}

type logs struct{ errors, infos []string }

func (l *logs) Error(args ...any) { l.errors = append(l.errors, fmt.Sprint(args...)) }
func (l *logs) Info(args ...any)  { l.infos = append(l.infos, fmt.Sprint(args...)) }
func (l *logs) Debug(args ...any) {}

func TestStartRefusesAServerBelow24_9(t *testing.T) {
	l := &logs{}
	n := Start(context.Background(), fakeStore{version: "24.8.1.1"}, Config{Enabled: true, Interval: time.Millisecond, MaxSpans: 1}, l)
	if n != nil || len(l.errors) != 1 || !strings.Contains(l.errors[0], "24.9") {
		t.Errorf("numberer %v, errors %q; want none started and one error naming 24.9", n, l.errors)
	}
}

func TestStartRunsPassesUntilStopped(t *testing.T) {
	l := &logs{}
	srv := fakeStore{version: "24.9.3.128", passes: make(chan struct{}, 100)}
	n := Start(context.Background(), srv, Config{Enabled: true, Idle: time.Second, Interval: time.Millisecond, MaxSpans: 1}, l)
	if n == nil {
		t.Fatalf("not started: %q", l.errors)
	}
	for range 3 {
		select {
		case <-srv.passes:
		case <-time.After(5 * time.Second):
			t.Fatal("no pass ran")
		}
	}
	n.Stop()
	for len(srv.passes) > 0 {
		<-srv.passes
	}
	time.Sleep(20 * time.Millisecond)
	if len(srv.passes) != 0 {
		t.Error("a pass ran after Stop")
	}
}

type fakeStore struct {
	version string
	passes  chan struct{}
}

func (f fakeStore) Version(context.Context) (string, error) { return f.version, nil }

func (f fakeStore) Pass(context.Context, time.Duration, int) error {
	select {
	case f.passes <- struct{}{}:
	default:
	}
	return nil
}
