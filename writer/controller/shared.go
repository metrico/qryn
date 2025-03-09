package controllerv1

import (
	"github.com/gofiber/fiber/v2"
	"github.com/metrico/qryn/writer/service"
	"github.com/metrico/qryn/writer/utils/httpresponse"
	"github.com/metrico/qryn/writer/utils/logger"
	"github.com/metrico/qryn/writer/utils/stat"
	"net/http"
	"runtime/debug"
	"strings"
)

func watchErr(err error) bool {
	if err == nil {
		return true
	}
	strErr := err.Error()
	if strings.HasPrefix(strErr, "json parse error") {
		stat.AddSentMetrics("json_parse_errors", 1)
		return true
	}
	if strings.Contains(strErr, "connection reset by peer") {
		stat.AddSentMetrics("connection_reset_by_peer", 1)
		return true
	}
	return false
}

func tamePanic(ctx *fiber.Ctx) {
	if err := recover(); err != nil {
		logger.Error(err, " stack:", string(debug.Stack()))
		httpresponse.CreateBadResponse(ctx, 500, "internal server error")
	}
}

func getAsyncMode(r *http.Request) int {
	header := r.Header.Get("X-Async-Insert")
	switch header {
	case "0":
		return service.INSERT_MODE_SYNC
	case "1":
		return service.INSERT_MODE_ASYNC
	default:
		return service.INSERT_MODE_DEFAULT
	}
}

func badRequestError(message string) error {
	err := *fiber.ErrBadRequest
	err.Message = message
	return &err
}
