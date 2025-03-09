package commonroutes

import (
	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// RegisterCommonRoutes registers the common routes to the given mux.
func RegisterCommonRoutes(app *mux.Router) {
	app.HandleFunc("/ready", Ready).Methods("GET")
	app.HandleFunc("/config", Config).Methods("GET")
	app.Handle("/metrics", promhttp.InstrumentMetricHandler(
		prometheus.DefaultRegisterer,
		promhttp.HandlerFor(prometheus.DefaultGatherer, promhttp.HandlerOpts{
			DisableCompression: true,
		}),
	)).Methods("GET")
	app.HandleFunc("/api/status/buildinfo", BuildInfo).Methods("GET")
}
