package main

import (
	"context"

	"github.com/redis/go-redis/v9"
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

// closeRedisConnections closes Redis remote connection
func closeRedisConnections(config Config) {
	rRemote.Close()
}
