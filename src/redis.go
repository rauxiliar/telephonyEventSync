package main

import (
	"context"

	"github.com/redis/go-redis/v9"
)

var (
	// Track if this is the initial connection
	isInitialConnection bool = true
)

// initializeRedisConnections initializes Redis remote connection
func initializeRedisConnections(ctx context.Context, config Config) error {
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
			LogInfo("Redis connection established")
			// Count active connections in the pool
			stats := rRemote.PoolStats()
			activeConnections := stats.TotalConns
			GetMetricsManager().SetRedisConnections(int64(activeConnections))

			// If this is not the initial connection, it's a reconnection
			if !isInitialConnection {
				LogInfo("Redis reconnection detected")
				GetMetricsManager().IncrementRedisReconnections()
			} else {
				// Mark that initial connection is done
				isInitialConnection = false
			}

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
