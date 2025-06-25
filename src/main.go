package main

import (
	"context"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/0x19/goesl"
	"github.com/redis/go-redis/v9"
)

var (
	// Redis client
	rRemote *redis.Client

	// Metrics
	metrics = &Metrics{
		lastSyncTime: time.Now(),
	}

	// Health monitor
	healthMonitor *HealthMonitor

	// Global channels for metrics
	globalReaderChan chan *goesl.Message
	globalWriterChan chan message
)

func main() {
	// Create context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize logger
	initLogger()
	defer globalLogger.Shutdown()

	// Load configuration
	config := getConfig()
	if err := validateConfig(&config); err != nil {
		LogError("Invalid configuration: %v", err)
		os.Exit(1)
	}

	// Initialize health monitor
	healthMonitor = NewHealthMonitor(config)
	healthMonitor.Start()

	// Initialize Redis connections
	if err := initializeRedisConnections(ctx, config); err != nil {
		LogError("%v", err)
		os.Exit(1)
	}

	// Initialize retry queue
	initializeRetryQueue(config)

	ch := make(chan message, config.Processing.BufferSize)
	globalWriterChan = ch // Set global writer channel for metrics
	var wg sync.WaitGroup

	// Channel for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Start multiple readers and writers
	wg.Add(1)
	go StartESLConnection(ctx, ch, &wg, config)

	wg.Add(1 + config.Processing.WriterWorkers)
	for i := range config.Processing.WriterWorkers {
		go writer(ctx, ch, &wg, i, config)
	}
	go printMetrics()

	// Start HTTP server for health checks and metrics
	go startAPIServer()

	// Start latency checker
	startLatencyChecker(config)

	// Start retry worker
	startRetryWorker(ctx, config)

	// Start trim routine (needed for all reader types)
	go trimStreams(ctx, config)

	// Wait for interrupt signal
	<-sigChan
	LogInfo("Starting graceful shutdown...")

	// Stop health monitor
	healthMonitor.Stop()

	// Close message channel
	close(ch)

	// Close connections (handles metrics internally)
	closeESLConnections()
	closeRedisConnections()

	// Wait for goroutines to finish
	wg.Wait()

	// Wait for retry worker to finish
	retryWg.Wait()

	LogInfo("Shutdown complete")
}
