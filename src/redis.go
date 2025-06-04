package main

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// initializeRedisConnections initializes both local and remote Redis connections
func initializeRedisConnections(ctx context.Context, config Config) error {
	// Connect to remote Redis (needed for all reader types)
	rRemote = redis.NewClient(&redis.Options{
		Addr:         config.Redis.Remote.Address,
		Password:     config.Redis.Remote.Password,
		DB:           config.Redis.Remote.DB,
		PoolSize:     config.Redis.Remote.PoolSize,
		MinIdleConns: config.Redis.Remote.MinIdleConns,
		MaxRetries:   config.Redis.Remote.MaxRetries,
	})
	// Verify remote Redis connection
	if err := rRemote.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("error connecting to remote Redis: %v", err)
	}

	// If using Redis reader, also initialize local Redis
	if config.Reader.Type == "redis" {
		rLocal = redis.NewClient(&redis.Options{
			Addr:         config.Redis.Local.Address,
			Password:     config.Redis.Local.Password,
			DB:           config.Redis.Local.DB,
			PoolSize:     config.Redis.Local.PoolSize,
			MinIdleConns: config.Redis.Local.MinIdleConns,
			MaxRetries:   config.Redis.Local.MaxRetries,
		})
		// Verify local Redis connection
		if err := rLocal.Ping(ctx).Err(); err != nil {
			return fmt.Errorf("error connecting to local Redis: %v", err)
		}

		// Create groups in local streams
		streams := []string{config.Streams.Events.Name, config.Streams.Jobs.Name}
		for _, stream := range streams {
			if err := createGroup(ctx, rLocal, stream, config.Redis.Group); err != nil {
				return fmt.Errorf("error creating group in stream %s: %v", stream, err)
			}
		}
	}

	return nil
}

// closeRedisConnections closes Redis connections based on reader type
func closeRedisConnections(config Config) {
	if config.Reader.Type == "redis" {
		rLocal.Close()
	}
	rRemote.Close()
}
