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

type Metrics struct {
	sync.Mutex
	messagesProcessed int64
	errors            int64
	lastSyncTime      time.Time
	queueSize         int
}

type message struct {
	stream         string
	id             string
	values         map[string]string
	readTime       time.Time // Timestamp when the message was read and processed
	eventTimestamp int64     // Original event timestamp (in milliseconds)
}

var (
	// Redis clients
	rLocal  *redis.Client
	rRemote *redis.Client

	// Metrics
	metrics = &Metrics{
		lastSyncTime: time.Now(),
	}

	// Health monitor
	healthMonitor *HealthMonitor
)

func createGroup(ctx context.Context, client *redis.Client, stream string, groupName string) error {
	// Try to create the group, if it exists, that's fine
	err := client.XGroupCreateMkStream(ctx, stream, groupName, "0").Err()
	if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
		LogError("Failed to create group %s in stream %s: %v", groupName, stream, err)
		return err
	}
	LogInfo("Group %s created/verified in stream %s", groupName, stream)
	return nil
}

func printMetrics() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		metrics.Lock()
		LogInfo("Messages processed (last 5s): %d, Errors: %d, Queue size: %d, Last sync: %v",
			metrics.messagesProcessed,
			metrics.errors,
			metrics.queueSize,
			metrics.lastSyncTime.Format(time.RFC3339))

		// Reset counter after each print
		metrics.messagesProcessed = 0
		metrics.Unlock()
	}
}

func trimStreams(ctx context.Context, config Config) {
	ticker := time.NewTicker(config.Processing.TrimInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Get current Redis time
			timeCmd := rLocal.Time(ctx)
			if timeCmd.Err() != nil {
				LogError("Failed to get Redis time: %v", timeCmd.Err())
				continue
			}

			// Calculate trim times
			now := time.Now()
			eventsTrimTime := now.Add(-config.Streams.Events.ExpireTime).UnixMilli()
			jobsTrimTime := now.Add(-config.Streams.Jobs.ExpireTime).UnixMilli()

			// Trim events stream
			eventsResult, err := rRemote.XTrimMinID(ctx, config.Streams.Events.Name, fmt.Sprintf("%d-0", eventsTrimTime)).Result()
			if err != nil {
				LogError("Failed to trim events stream: %v", err)
			} else {
				LogDebug("Trimmed %d entries from %s", eventsResult, config.Streams.Events.Name)
			}

			// Trim jobs stream
			jobsResult, err := rRemote.XTrimMinID(ctx, config.Streams.Jobs.Name, fmt.Sprintf("%d-0", jobsTrimTime)).Result()
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

	// Initialize health monitor
	healthMonitor = NewHealthMonitor(config)
	healthMonitor.Start()

	// Initialize Redis connections
	if err := initializeRedisConnections(ctx, config); err != nil {
		LogError("%v", err)
		os.Exit(1)
	}

	ch := make(chan message, config.Processing.BufferSize)
	var wg sync.WaitGroup

	// Channel for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Start multiple readers and multiple writers
	wg.Add(1 + config.Processing.ReaderWorkers)

	var cleanup *ConsumerCleanup

	switch config.Reader.Type {
	case "redis":
		// Initialize cleanup mechanism
		streams := []string{config.Streams.Events.Name, config.Streams.Jobs.Name}
		cleanup = NewConsumerCleanup(rLocal, config.Redis.Group, config.Redis.Consumer, streams, config.Processing.ReaderWorkers)
		cleanup.Start()

		// Start multiple readers and multiple writers
		for i := range config.Processing.ReaderWorkers {
			configCopy := config
			configCopy.Redis.Consumer = fmt.Sprintf("%s_%d", config.Redis.Consumer, i)
			go reader(ctx, ch, &wg, i, configCopy)
		}

	case "unix":
		go unixSocketServer(ctx, ch, &wg, config)
	case "esl":
		for range config.Processing.ReaderWorkers {
			configCopy := config
			go eslSocketServer(ctx, ch, &wg, configCopy)
		}
	default:
		LogError("Invalid reader type: %s. Use 'redis', 'unix' or 'esl'", config.Reader.Type)
		os.Exit(1)
	}

	wg.Add(1 + config.Processing.WriterWorkers)
	for i := range config.Processing.WriterWorkers {
		go writer(ctx, ch, &wg, i, config)
	}
	go printMetrics()

	// Start HTTP server for health checks
	go startHealthServer()

	// Start latency checker
	startLatencyChecker(config)

	// Start trim routine (needed for all reader types)
	go trimStreams(ctx, config)

	// Wait for interrupt signal
	<-sigChan
	LogInfo("Starting graceful shutdown...")

	// Stop health monitor
	healthMonitor.Stop()

	// Signal reader to stop first
	close(stopChan)

	// Wait a bit for reader to stop
	time.Sleep(100 * time.Millisecond)

	// Close message channel
	close(ch)

	// Stop cleanup if Redis reader
	if config.Reader.Type == "redis" {
		cleanup.Stop()
	}

	// Close Redis connections
	closeRedisConnections(config)

	// Wait for goroutines to finish
	wg.Wait()

	LogInfo("Shutdown complete")
}
