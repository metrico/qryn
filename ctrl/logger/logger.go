package logger

import (
	clconfig "github.com/metrico/cloki-config"
	"github.com/sirupsen/logrus"
	"io"
	"log"
	"os"
)

type ILogger interface {
	Error(args ...any)
	Debug(args ...any)
	Info(args ...any)
}

var Logger = logrus.New()

// initLogger function
func InitLogger(config *clconfig.ClokiConfig, output io.Writer) {

	//env := os.Getenv("environment")
	//isLocalHost := env == "local"
	if config.Setting.LOG_SETTINGS.Json {
		// Log as JSON instead of the default ASCII formatter.
		Logger.SetFormatter(&logrus.JSONFormatter{})
	} else {
		Logger.Formatter.(*logrus.TextFormatter).DisableTimestamp = false
		Logger.Formatter.(*logrus.TextFormatter).DisableColors = true
	}
	// Output to stdout instead of the default stderr
	// Can be any io.Writer, see below for File example
	if output != nil {
		Logger.SetOutput(output)
		log.SetOutput(output)
	} else if config.Setting.LOG_SETTINGS.Stdout {
		Logger.SetOutput(os.Stdout)
		log.SetOutput(os.Stdout)
	}

	/* log level default */
	if config.Setting.LOG_SETTINGS.Level == "" {
		config.Setting.LOG_SETTINGS.Level = "error"
	}

	if logLevel, ok := logrus.ParseLevel(config.Setting.LOG_SETTINGS.Level); ok == nil {
		// Only log the warning severity or above.
		Logger.SetLevel(logLevel)
	} else {
		Logger.Error("Couldn't parse loglevel", config.Setting.LOG_SETTINGS.Level)
		Logger.SetLevel(logrus.ErrorLevel)
	}

	Logger.Info("init logging system")
}

func Debug(args ...any) {
	Logger.Debug(args...)
}

func Info(args ...any) {
	Logger.Info(args...)
}

func Error(args ...any) {
	Logger.Error(args...)
}
