package reader

import (
	_ "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/gorilla/mux"
	_ "github.com/gorilla/mux"
	jsoniter "github.com/json-iterator/go"
	clconfig "github.com/metrico/cloki-config"
	"github.com/metrico/qryn/reader/config"
	"github.com/metrico/qryn/reader/dbRegistry"
	"github.com/metrico/qryn/reader/model"
	apirouterv1 "github.com/metrico/qryn/reader/router"
	"github.com/metrico/qryn/reader/utils/logger"
	"github.com/metrico/qryn/reader/utils/middleware"
	"github.com/metrico/qryn/reader/watchdog"
	"net"
	"net/http"
	"runtime"
)

var ownHttpServer bool = false

func Init(cnf *clconfig.ClokiConfig, app *mux.Router) {
	config.Cloki = cnf

	//Set to max cpu if the value is equals 0
	if config.Cloki.Setting.SYSTEM_SETTINGS.CPUMaxProcs == 0 {
		runtime.GOMAXPROCS(runtime.NumCPU())
	} else {
		runtime.GOMAXPROCS(config.Cloki.Setting.SYSTEM_SETTINGS.CPUMaxProcs)
	}

	// initialize logger
	//
	logger.InitLogger()

	if app == nil {
		app = mux.NewRouter()
		ownHttpServer = true
	}

	//Api
	// configure to serve WebServices
	configureAsHTTPServer(app)
}

func configureAsHTTPServer(acc *mux.Router) {
	//httpURL := fmt.Sprintf("%s:%d", config.Cloki.Setting.HTTP_SETTINGS.Host, config.Cloki.Setting.HTTP_SETTINGS.Port)
	httpURL := func() string {
		stream := jsoniter.ConfigFastest.BorrowStream(nil)
		defer jsoniter.ConfigFastest.ReturnStream(stream)
		stream.WriteRaw(config.Cloki.Setting.HTTP_SETTINGS.Host)
		stream.WriteRaw(":")
		// If Port is int, convert it to int64 first.
		stream.WriteInt64(int64(config.Cloki.Setting.HTTP_SETTINGS.Port))
		return string(stream.Buffer())
	}()
	applyMiddlewares(acc)

	performV1APIRouting(acc)

	if ownHttpServer {
		httpStart(acc, httpURL)
	}
}

func applyMiddlewares(acc *mux.Router) {
	if !ownHttpServer {
		return
	}
	if config.Cloki.Setting.AUTH_SETTINGS.BASIC.Username != "" &&
		config.Cloki.Setting.AUTH_SETTINGS.BASIC.Password != "" {
		acc.Use(middleware.BasicAuthMiddleware(config.Cloki.Setting.AUTH_SETTINGS.BASIC.Username,
			config.Cloki.Setting.AUTH_SETTINGS.BASIC.Password))
	}
	acc.Use(middleware.AcceptEncodingMiddleware)
	if config.Cloki.Setting.HTTP_SETTINGS.Cors.Enable {
		acc.Use(middleware.CorsMiddleware(config.Cloki.Setting.HTTP_SETTINGS.Cors.Origin))
	}
	acc.Use(middleware.LoggingMiddleware("[{{.status}}] {{.method}} {{.url}} - LAT:{{.latency}}"))
}

func httpStart(server *mux.Router, httpURL string) {
	logger.Info("Starting service")
	http.Handle("/", server)
	listener, err := net.Listen("tcp", httpURL)
	if err != nil {
		logger.Error("Error creating listener:", err)
		panic(err)
	}
	logger.Info("Server is listening on", httpURL)
	if err := http.Serve(listener, server); err != nil {
		logger.Error("Error serving:", err)
		panic(err)
	}
}

func performV1APIRouting(acc *mux.Router) {
	dbRegistry.Init()
	watchdog.Init(&model.ServiceData{Session: dbRegistry.Registry})

	apirouterv1.RouteQueryRangeApis(acc, dbRegistry.Registry)
	apirouterv1.RouteSelectLabels(acc, dbRegistry.Registry)
	apirouterv1.RouteSelectPrometheusLabels(acc, dbRegistry.Registry)
	apirouterv1.RoutePrometheusQueryRange(acc, dbRegistry.Registry, config.Cloki.Setting.SYSTEM_SETTINGS.QueryStats)
	apirouterv1.RouteTempo(acc, dbRegistry.Registry)
	apirouterv1.RouteMiscApis(acc)
	apirouterv1.RouteProf(acc, dbRegistry.Registry)
	apirouterv1.PluggableRoutes(acc, dbRegistry.Registry)
}
