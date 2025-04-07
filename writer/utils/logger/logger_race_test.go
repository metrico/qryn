package logger

import (
	"fmt"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
	"testing"
)

func TestLoggerRaceCond(t *testing.T) {
	Logger.SetFormatter(&logrus.JSONFormatter{})
	qrynFmt := &qrynFormatter{
		formatter: Logger.Formatter,
		url:       "",
		app:       "",
		hostname:  "a",
		headers:   nil,
	}
	qrynFmt.Run()
	Logger.SetFormatter(qrynFmt)
	g := errgroup.Group{}
	for i := 0; i < 10; i++ {
		g.Go(func() error {
			for j := 0; j < 100000; j++ {
				Logger.Info("a", "B", fmt.Errorf("aaaa"))
			}
			return nil
		})
	}
	g.Wait()
}
