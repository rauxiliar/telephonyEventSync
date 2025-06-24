package main

import (
	"context"
	"fmt"
	"time"
)

// trimStreams periodically trims old entries from Redis streams
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
