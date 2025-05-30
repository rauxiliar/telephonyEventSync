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

	// Connect to local and remote Redis with optimized settings
	rLocal = redis.NewClient(&redis.Options{
		Addr:         config.Redis.Local.Address,
		Password:     config.Redis.Local.Password,
		DB:           config.Redis.Local.DB,
		PoolSize:     config.Redis.Local.PoolSize,
		MinIdleConns: config.Redis.Local.MinIdleConns,
		MaxRetries:   config.Redis.Local.MaxRetries,
	})
	rRemote = redis.NewClient(&redis.Options{
		Addr:         config.Redis.Remote.Address,
		Password:     config.Redis.Remote.Password,
		DB:           config.Redis.Remote.DB,
		PoolSize:     config.Redis.Remote.PoolSize,
		MinIdleConns: config.Redis.Remote.MinIdleConns,
		MaxRetries:   config.Redis.Remote.MaxRetries,
	})
	// Verify connections
	if err := rLocal.Ping(ctx).Err(); err != nil {
		LogError("Error connecting to local Redis: %v", err)
		os.Exit(1)
	}
	if err := rRemote.Ping(ctx).Err(); err != nil {
		LogError("Error connecting to remote Redis: %v", err)
		os.Exit(1)
	}

	// Create groups in local streams
	for _, stream := range config.Streams {
		if err := createGroup(ctx, rLocal, stream, config.Redis.Group); err != nil {
			LogError("Error creating group in stream %s: %v", stream, err)
			os.Exit(1)
		}
	}

	// Initialize cleanup mechanism
	cleanup := NewConsumerCleanup(rLocal, config.Redis.Group, config.Redis.Consumer, config.Streams, config.Processing.ReaderWorkers)
	cleanup.Start()

	ch := make(chan message, config.Processing.BufferSize)
	var wg sync.WaitGroup

	// Channel for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Start multiple readers and multiple writers
	wg.Add(1 + config.Processing.ReaderWorkers)
	for i := range config.Processing.ReaderWorkers {
		configCopy := config
		configCopy.Redis.Consumer = fmt.Sprintf("%s_%d", config.Redis.Consumer, i)
		go reader(ctx, ch, &wg, i, configCopy)
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

	// Stop cleanup
	cleanup.Stop()

	// Wait for goroutines to finish
	wg.Wait()

	// Close Redis connections
	rLocal.Close()
	rRemote.Close()

	LogInfo("Shutdown complete")
}
