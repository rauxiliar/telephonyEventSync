package main

import (
	"context"
	"sync"

	"github.com/redis/go-redis/v9"
)

var (
	// Track connection state
	redisConnectionState struct {
		sync.Mutex
		initialConnectionsMade int
		isInitializing         bool
	}
)

// initializeRedisConnections initializes Redis remote connection
// It initializes the Redis connection state
// It connects to Redis with configurable timeouts
// It verifies the Redis connection
// It returns an error if the connection fails
func initializeRedisConnections(ctx context.Context, config Config) error {
	redisConnectionState.Lock()
	redisConnectionState.isInitializing = true
	redisConnectionState.initialConnectionsMade = 0
	redisConnectionState.Unlock()

	// Connect to remote Redis with configurable timeouts
	rRemote = redis.NewClient(&redis.Options{
		Addr:         config.Redis.Remote.Address,
		Password:     config.Redis.Remote.Password,
		DB:           config.Redis.Remote.DB,
		PoolSize:     config.Redis.Remote.PoolSize,
		MinIdleConns: config.Redis.Remote.MinIdleConns,
		MaxRetries:   config.Redis.Remote.MaxRetries,
		// Timeout settings from config
		DialTimeout:  config.Redis.Remote.DialTimeout,
		WriteTimeout: config.Redis.Remote.WriteTimeout,
		PoolTimeout:  config.Redis.Remote.PoolTimeout,
		// Add hooks to detect reconnections
		OnConnect: func(ctx context.Context, cn *redis.Conn) error {
			return handleRedisConnection(config)
		},
	})

	// Verify remote Redis connection with timeout
	return verifyRedisConnection(ctx, config)
}

// handleRedisConnection handles Redis connection events and updates metrics
// It locks the Redis connection state
// It handles the Redis connection event
// It updates the Redis connection metrics
// It returns an error if the connection fails
func handleRedisConnection(config Config) error {
	redisConnectionState.Lock()
	defer redisConnectionState.Unlock()

	handleRedisConnectionEvent(config)
	updateRedisConnectionMetrics()

	return nil
}

// handleRedisConnectionEvent processes connection events based on initialization state
// It handles the Redis connection event
// It handles the Redis initial connection
// It handles the Redis reconnection
func handleRedisConnectionEvent(config Config) {
	// If we're still initializing, these are initial pool connections
	if redisConnectionState.isInitializing {
		handleRedisInitialConnection(config)
	} else {
		// This is a real reconnection (after initialization is complete)
		LogInfo("Redis reconnection detected")
		GetMetricsManager().IncrementRedisReconnections()
	}
}

// handleRedisInitialConnection handles initial pool connections during startup
// It increments the initial connection count
// It logs the initial connection
// It marks the initialization as complete when the minimum idle connections are reached
func handleRedisInitialConnection(config Config) {
	redisConnectionState.initialConnectionsMade++

	// Log only the first connection during initialization
	if redisConnectionState.initialConnectionsMade == 1 {
		LogInfo("Redis connection established")
	}

	// Mark initialization as complete when we have enough connections for the pool
	if redisConnectionState.initialConnectionsMade >= config.Redis.Remote.MinIdleConns {
		redisConnectionState.isInitializing = false
	}
}

// getAndUpdateRedisConnectionMetrics updates metrics with current pool stats
// It gets the current Redis connection pool statistics
// It updates the Redis connection metrics
func updateRedisConnectionMetrics() {
	stats := rRemote.PoolStats()
	activeConnections := stats.TotalConns
	GetMetricsManager().SetRedisConnections(int64(activeConnections))
}

// verifyRedisConnection verifies the Redis connection with a ping
// It creates a context with timeout for the ping
// It verifies the Redis connection
// It returns an error if the connection fails
func verifyRedisConnection(ctx context.Context, config Config) error {
	pingCtx, cancel := context.WithTimeout(ctx, config.Redis.Remote.DialTimeout)
	defer cancel()

	if err := rRemote.Ping(pingCtx).Err(); err != nil {
		return &RedisConnectionError{
			Address: config.Redis.Remote.Address,
			Err:     err,
		}
	}

	return nil
}

// closeRedisConnections closes Redis remote connection and updates metrics
// It logs the Redis connection statistics
// It closes the Redis connection
// It updates the Redis connection metrics
func closeRedisConnections() {
	if rRemote != nil {
		logRedisConnectionStats()
		rRemote.Close()
		GetMetricsManager().SetRedisConnections(0)
		LogInfo("Redis connections closed")
	}
}

// logRedisConnectionStats logs the current connection pool statistics
// It gets the current Redis connection pool statistics
// It logs the Redis connection statistics
func logRedisConnectionStats() {
	stats := rRemote.PoolStats()
	LogInfo("Closing Redis connections. Pool stats: Total=%d, Idle=%d, Stale=%d",
		stats.TotalConns, stats.IdleConns, stats.StaleConns)
}
