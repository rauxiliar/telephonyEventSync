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
	// Create a cancellable context for graceful shutdown
	// This context will be cancelled when shutdown is requested
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize the global logger with configuration
	// Must be done first as other components depend on logging
	initLogger()
	defer globalLogger.Shutdown()

	// Load and validate configuration from environment variables and files
	// Exits with error code 1 if configuration is invalid
	config := loadAndValidateConfig()

	// Create and start the health monitor to check Redis and ESL connections
	// Runs in background goroutine, monitors system health periodically
	healthMonitor = NewHealthMonitor(config)
	healthMonitor.Start()

	// Initialize Redis connections (remote and local)
	// Exits with error code 1 if Redis connection fails
	if err := initializeRedisConnections(ctx, config); err != nil {
		LogError("%v", err)
		os.Exit(1)
	}

	// Create retry queue channel for failed messages
	// Messages that fail to be written to Redis are queued here for retry
	retryQueue = make(chan message, config.Processing.WriterRetryQueueSize)

	// Create main message channel between ESL reader and Redis writers
	// This is the primary data flow channel for processed events
	ch := make(chan message, config.Processing.BufferSize)
	globalWriterChan = ch
	var wg sync.WaitGroup

	// Set up signal handling for graceful shutdown
	// Listens for SIGINT (Ctrl+C) and SIGTERM signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Start all main worker goroutines:
	// - ESL connection reader (reads events from FreeSWITCH)
	// - Multiple Redis writers (write events to Redis streams)
	startWorkers(ctx, ch, &wg, config)

	// Start retry worker goroutine
	// Processes failed messages from retry queue
	startRetryWorker(ctx, config)

	// Start metrics printing goroutine
	// Periodically prints system metrics to logs
	go printMetrics()

	// Start API server goroutine
	// Provides HTTP endpoints for health checks and metrics
	go startAPIServer()

	// Start latency checker (runs in main thread, not goroutine)
	// Calculates total latency from event generation to Redis storage
	go startLatencyChecker(config)

	// Start stream trimming goroutine
	// Periodically trims Redis streams to maintain size limits
	go trimStreams(ctx, config)

	// Wait for interrupt signal (SIGINT or SIGTERM)
	// BLOCKING: This blocks until shutdown signal is received
	<-sigChan
	LogInfo("Starting graceful shutdown...")

	// Perform graceful shutdown:
	// - Stop health monitor
	// - Close channels
	// - Close connections
	// - Wait for all goroutines to complete
	shutdown(ch, &wg)
	LogInfo("Shutdown complete")
}

// Loads configuration and validates it, exiting on error
func loadAndValidateConfig() Config {
	config := getConfig()
	if err := validateConfig(&config); err != nil {
		LogError("Invalid configuration: %v", err)
		os.Exit(1)
	}
	return config
}

// Starts all main worker goroutines (readers, writers)
// It starts the ESL connection reader
// It starts the Redis writers
func startWorkers(ctx context.Context, ch chan message, wg *sync.WaitGroup, config Config) {
	wg.Add(1)
	go StartESLConnection(ctx, ch, wg, config)

	wg.Add(1 + config.Processing.WriterWorkers)
	for i := range config.Processing.WriterWorkers {
		go writer(ctx, ch, wg, i, config)
	}
}

// Performs shutdown: stops health monitor, closes channels/connections, waits for goroutines
// It stops the health monitor
// It closes the channels
// It closes the ESL connections
// It closes the Redis connections
// It waits for all main worker goroutines to complete
// It waits for the retry worker goroutine to complete
func shutdown(ch chan message, wg *sync.WaitGroup) {
	healthMonitor.Stop()
	close(ch)
	closeESLConnections()
	closeRedisConnections()
	// BLOCKING: Wait for all main worker goroutines to complete
	wg.Wait()
	// BLOCKING: Wait for retry worker goroutine to complete
	retryWg.Wait()
}
