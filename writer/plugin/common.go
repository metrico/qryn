package plugin

import (
	"github.com/metrico/cloki-config/config"
	"github.com/metrico/qryn/writer/model"
	"github.com/metrico/qryn/writer/service"
)

type InsertServiceFactory interface {
	// Create the TimeSeries Insert Service
	NewTimeSeriesInsertService(opts model.InsertServiceOpts) service.IInsertServiceV2
	// Similarly, create methods for other services if needed
	NewSamplesInsertService(opts model.InsertServiceOpts) service.IInsertServiceV2
	NewMetricsInsertService(opts model.InsertServiceOpts) service.IInsertServiceV2
	NewTempoSamplesInsertService(opts model.InsertServiceOpts) service.IInsertServiceV2
	NewTempoTagInsertService(opts model.InsertServiceOpts) service.IInsertServiceV2
	NewProfileSamplesInsertService(opts model.InsertServiceOpts) service.IInsertServiceV2
	// Add other service creation methods here if necessary
}

type ConfigInitializer interface {
	InitializeConfig(conf *config.ClokiBaseSettingServer) error
}
