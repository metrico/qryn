package main

import (
	"fmt"
	"github.com/grafana/pyroscope-go"
	"log"
	"os"
)

func initPyro() {
	// Pyroscope configuration
	serverAddress := os.Getenv("PYROSCOPE_SERVER_ADDRESS")
	if serverAddress == "" {
		return
	}

	applicationName := os.Getenv("PYROSCOPE_APPLICATION_NAME")
	if applicationName == "" {
		applicationName = "gigapipe"
	}

	// Initialize Pyroscope
	config := pyroscope.Config{
		ApplicationName: applicationName,
		ServerAddress:   serverAddress,
		Logger:          pyroscope.StandardLogger,
		ProfileTypes: []pyroscope.ProfileType{
			pyroscope.ProfileCPU,
			pyroscope.ProfileAllocObjects,
			pyroscope.ProfileAllocSpace,
			pyroscope.ProfileInuseObjects,
			pyroscope.ProfileInuseSpace,
		},
	}

	_, err := pyroscope.Start(config)
	if err != nil {
		log.Fatalf("Failed to start Pyroscope: %v", err)
	}
	fmt.Println("Pyroscope profiling started")
}
