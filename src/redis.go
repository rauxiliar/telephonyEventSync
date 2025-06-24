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
func initializeRedisConnections(ctx context.Context, config Config) error {
	// Mark that we're in initialization phase
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
			redisConnectionState.Lock()
			defer redisConnectionState.Unlock()

			// If we're still initializing, these are initial pool connections
			if redisConnectionState.isInitializing {
				redisConnectionState.initialConnectionsMade++

				// Log only the first connection during initialization
				if redisConnectionState.initialConnectionsMade == 1 {
					LogInfo("Redis connection established")
				}

				// Mark initialization as complete when we have enough connections for the pool
				if redisConnectionState.initialConnectionsMade >= config.Redis.Remote.MinIdleConns {
					redisConnectionState.isInitializing = false
				}
			} else {
				// This is a real reconnection (after initialization is complete)
				LogInfo("Redis reconnection detected")
				GetMetricsManager().IncrementRedisReconnections()
			}

			// Update metrics with current pool stats
			stats := rRemote.PoolStats()
			activeConnections := stats.TotalConns
			GetMetricsManager().SetRedisConnections(int64(activeConnections))

			return nil
		},
	})

	// Verify remote Redis connection with timeout
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
func closeRedisConnections() {
	if rRemote != nil {
		// Get stats before closing
		stats := rRemote.PoolStats()
		LogInfo("Closing Redis connections. Pool stats: Total=%d, Idle=%d, Stale=%d",
			stats.TotalConns, stats.IdleConns, stats.StaleConns)

		rRemote.Close()
		GetMetricsManager().SetRedisConnections(0)
		LogInfo("Redis connections closed")
	}
}
