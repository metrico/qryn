package apirouterv1

import (
	"github.com/gorilla/mux"
	controllerv1 "github.com/metrico/qryn/reader/controller"
	"github.com/metrico/qryn/reader/model"
	"github.com/metrico/qryn/reader/service"
)

func RouteQueryRangeApis(app *mux.Router, dataSession model.IDBRegistry) {
	qrService := &service.QueryRangeService{
		ServiceData: model.ServiceData{
			Session: dataSession,
		},
	}
	qrCtrl := &controllerv1.QueryRangeController{
		QueryRangeService: qrService,
	}
	app.HandleFunc("/loki/api/v1/query_range", qrCtrl.QueryRange).Methods("GET")
	app.HandleFunc("/loki/api/v1/query", qrCtrl.Query).Methods("GET")
	app.HandleFunc("/loki/api/v1/tail", qrCtrl.Tail).Methods("GET")
}
