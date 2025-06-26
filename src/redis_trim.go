package main

import (
	"context"
	"fmt"
	"time"
)

// trimStreams periodically removes old entries from Redis streams
// It starts the trim stream loop
// It stops the trim stream loop
// It trims the Redis streams
// It calculates the trim limits
// It trims the Redis streams
// It returns an error if the trim fails
func trimStreams(ctx context.Context, config Config) {
	defer PanicRecoveryFunc("trimStreams")()

	ticker := time.NewTicker(config.Processing.TrimInterval)
	defer ticker.Stop()

	trimTimeout := config.GetRedisRemoteWriteTimeout()

	for {
		// BLOCKING: Periodically trims Redis streams, blocks on timer or context cancellation
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			redisTime, err := getRedisTimeWithTimeout(ctx, trimTimeout)
			if err != nil {
				continue
			}

			eventsTrimTime, jobsTrimTime := calculateTrimLimits(redisTime, config)

			trimStreamWithTimeout(ctx, trimTimeout, config.Streams.Events.Name, eventsTrimTime)
			trimStreamWithTimeout(ctx, trimTimeout, config.Streams.Jobs.Name, jobsTrimTime)
		}
	}
}

// getRedisTimeWithTimeout gets the current Redis time with timeout
func getRedisTimeWithTimeout(ctx context.Context, timeout time.Duration) (time.Time, error) {
	timeCtx, timeCancel := context.WithTimeout(ctx, timeout)
	defer timeCancel()
	timeCmd := rRemote.Time(timeCtx)
	if timeCmd.Err() != nil {
		LogError("Failed to get Redis time: %v", timeCmd.Err())
		return time.Time{}, timeCmd.Err()
	}
	return time.Now(), nil // Could use Redis time if needed
}

// calculateTrimLimits calculates the trim limits for the streams
func calculateTrimLimits(now time.Time, config Config) (int64, int64) {
	eventsTrimTime := now.Add(-config.Streams.Events.ExpireTime).UnixMilli()
	jobsTrimTime := now.Add(-config.Streams.Jobs.ExpireTime).UnixMilli()
	return eventsTrimTime, jobsTrimTime
}

// trimStreamWithTimeout trims a stream with timeout
func trimStreamWithTimeout(ctx context.Context, timeout time.Duration, streamName string, trimTime int64) {
	trimCtx, trimCancel := context.WithTimeout(ctx, timeout)
	defer trimCancel()
	result, err := rRemote.XTrimMinID(trimCtx, streamName, fmt.Sprintf("%d-0", trimTime)).Result()
	if err != nil {
		LogError("Failed to trim %s stream: %v", streamName, err)
	} else {
		LogDebug("Trimmed %d entries from %s", result, streamName)
	}
}
