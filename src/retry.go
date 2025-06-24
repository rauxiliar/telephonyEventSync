package main

import (
	"context"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// Global retry queue and worker
var (
	retryQueue chan message
	retryWg    sync.WaitGroup
)

// initializeRetryQueue initializes the retry queue with configurable size
func initializeRetryQueue(config Config) {
	retryQueue = make(chan message, config.Processing.WriterRetryQueueSize)
}

// startRetryWorker starts the background retry worker
func startRetryWorker(ctx context.Context, config Config) {
	retryWg.Add(1)
	go func() {
		defer retryWg.Done()
		retryWorker(ctx, config)
	}()
}

// retryWorker processes retry messages in background with round-robin approach
func retryWorker(ctx context.Context, config Config) {
	// Panic recovery for the retry worker
	defer PanicRecoveryFunc("retry worker")()

	for {
		select {
		case <-ctx.Done():
			return
		default:
			// Process one message from queue
			select {
			case msg := <-retryQueue:
				// Check TTL before retrying based on event timestamp
				eventTime := time.Unix(0, msg.eventTimestamp)
				if time.Since(eventTime) > config.Processing.WriterRetryTTL {
					LogWarn("Dropping message %s from retry queue due to TTL expiration (event age: %v)",
						msg.uuid, time.Since(eventTime))
					GetMetricsManager().IncrementErrors()
					continue
				}

				// Check if max retries reached
				if msg.retries >= config.Processing.WriterMaxRetries {
					LogError("Dropping message %s after %d retry attempts", msg.uuid, msg.retries)
					GetMetricsManager().IncrementErrors()
					continue
				}

				// Try to send the message
				if err := retryMessageOnce(ctx, msg, config); err != nil {
					// Increment retry counter
					msg.retries++

					// Re-queue the message for next round if not at max
					LogWarn("Retry attempt %d for message %s failed: %v", msg.retries, msg.uuid, err)
					select {
					case retryQueue <- msg:
						// Message re-queued successfully
					default:
						LogError("Retry queue full, message %s lost after %d attempts", msg.uuid, msg.retries)
						GetMetricsManager().IncrementErrors()
					}
				} else {
					LogDebug("Successfully retried message %s on attempt %d", msg.uuid, msg.retries+1)
				}

				// Pause after each attempt (successful or not)
				LogDebug("Pausing %v before next retry attempt", config.Processing.WriterRetryPauseAfter)
				timer := time.NewTimer(config.Processing.WriterRetryPauseAfter)
				select {
				case <-timer.C:
					// Timer expired, continue with next message
				case <-ctx.Done():
					timer.Stop()
					return
				}
			default:
				// No messages in queue, wait a bit
				select {
				case <-time.After(100 * time.Millisecond):
				case <-ctx.Done():
					return
				}
			}
		}
	}
}

// retryMessageOnce attempts to send a message once (no internal retries)
func retryMessageOnce(ctx context.Context, msg message, config Config) error {
	// Create context with timeout for retry
	retryCtx, cancel := context.WithTimeout(ctx, config.GetRedisRemoteWriteTimeout())
	defer cancel()

	maxLen := config.Streams.Events.MaxLen
	if msg.stream == config.Streams.Jobs.Name {
		maxLen = config.Streams.Jobs.MaxLen
	}

	// Try to add message directly
	_, err := rRemote.XAdd(retryCtx, &redis.XAddArgs{
		Stream: msg.stream,
		Values: msg.values,
		MaxLen: maxLen,
		Approx: true,
	}).Result()

	return err
}

// sendToRetryQueue sends a message to the retry queue
func sendToRetryQueue(msg message) bool {
	// Initialize retry counter if not set
	if msg.retries == 0 {
		msg.retries = 0 // Already 0, but explicit for clarity
	}
	select {
	case retryQueue <- msg:
		return true
	default:
		LogError("Retry queue full, message %s lost", msg.uuid)
		GetMetricsManager().IncrementErrors()
		return false
	}
}
