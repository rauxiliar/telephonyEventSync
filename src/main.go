package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

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
)

func printMetrics() {
	config := getConfig()
	ticker := time.NewTicker(config.GetMetricsPrintInterval())
	defer ticker.Stop()

	for range ticker.C {
		metricsManager := GetMetricsManager()
		snapshot := metricsManager.GetSnapshot()

		LogInfo("Messages processed (last 5s): %d, Errors: %d, Reader Channel: %d, Writer Channel: %d, Last sync: %v",
			snapshot.MessagesProcessed,
			snapshot.Errors,
			snapshot.ReaderChannelSize,
			snapshot.WriterChannelSize,
			snapshot.LastSyncTime.Format(time.RFC3339))

		// Reset counter after each print
		metricsManager.ResetCounters()
	}
}

func trimStreams(ctx context.Context, config Config) {
	// Panic recovery for the trim goroutine
	defer PanicRecoveryFunc("trimStreams")()

	ticker := time.NewTicker(config.Processing.TrimInterval)
	defer ticker.Stop()

	// Use configurable timeout for trim operations
	trimTimeout := config.GetRedisRemoteWriteTimeout()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Get current Redis time with timeout
			timeCtx, timeCancel := context.WithTimeout(ctx, trimTimeout)
			timeCmd := rRemote.Time(timeCtx)
			if timeCmd.Err() != nil {
				LogError("Failed to get Redis time: %v", timeCmd.Err())
				timeCancel()
				continue
			}
			timeCancel()

			// Calculate trim times
			now := time.Now()
			eventsTrimTime := now.Add(-config.Streams.Events.ExpireTime).UnixMilli()
			jobsTrimTime := now.Add(-config.Streams.Jobs.ExpireTime).UnixMilli()

			// Trim events stream with timeout
			eventsCtx, eventsCancel := context.WithTimeout(ctx, trimTimeout)
			eventsResult, err := rRemote.XTrimMinID(eventsCtx, config.Streams.Events.Name, fmt.Sprintf("%d-0", eventsTrimTime)).Result()
			eventsCancel()
			if err != nil {
				LogError("Failed to trim events stream: %v", err)
			} else {
				LogDebug("Trimmed %d entries from %s", eventsResult, config.Streams.Events.Name)
			}

			// Trim jobs stream with timeout
			jobsCtx, jobsCancel := context.WithTimeout(ctx, trimTimeout)
			jobsResult, err := rRemote.XTrimMinID(jobsCtx, config.Streams.Jobs.Name, fmt.Sprintf("%d-0", jobsTrimTime)).Result()
			jobsCancel()
			if err != nil {
				LogError("Failed to trim jobs stream: %v", err)
			} else {
				LogDebug("Trimmed %d entries from %s", jobsResult, config.Streams.Jobs.Name)
			}
		}
	}
}

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
	var wg sync.WaitGroup

	// Channel for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Start multiple readers and writers
	wg.Add(1 + config.Processing.ReaderWorkers)
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

	// Close Redis connections
	closeRedisConnections(config)

	// Wait for goroutines to finish
	wg.Wait()

	// Wait for retry worker to finish
	retryWg.Wait()

	LogInfo("Shutdown complete")
}
