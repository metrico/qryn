package shared

import (
	"fmt"
	"github.com/metrico/qryn/reader/utils/logger"
	"runtime/debug"
)

type NotSupportedError struct {
	Msg string
}

func (n *NotSupportedError) Error() string {
	return n.Msg
}

func isNotSupportedError(e error) bool {
	_, ok := e.(*NotSupportedError)
	return ok
}

func TamePanic(out chan []LogEntry) {
	if err := recover(); err != nil {
		logger.Error(err, " stack:", string(debug.Stack()))
		out <- []LogEntry{{Err: fmt.Errorf("panic: %v", err)}}
		recover()
	}
}
