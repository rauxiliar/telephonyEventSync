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

// startRetryWorker starts the background retry worker
// It adds the retry worker to the wait group
// It starts the retry worker goroutine
func startRetryWorker(ctx context.Context, config Config) {
	retryWg.Add(1)
	go func() {
		defer retryWg.Done()
		retryWorker(ctx, config)
	}()
}

// retryWorker processes retry messages in background with round-robin approach
// It processes retry messages in background with round-robin approach
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
				if shouldProcessMessage(msg, config) {
					// Process the retry message: attempt to send it to Redis
					// If it fails, the message will be re-queued with incremented retry counter
					processRetryMessage(ctx, msg, config)
					// Pause after processing to avoid overwhelming Redis with rapid retry attempts
					// This implements a backoff strategy to give Redis time to recover
					pauseAfterProcessing(ctx, config)
				}
			default:
				select {
				// BLOCKING: Wait 100ms when no messages are available to avoid busy waiting
				case <-time.After(100 * time.Millisecond):
				case <-ctx.Done():
				}
			}
		}
	}
}

// shouldProcessMessage validates if a message should be processed
// It checks if the message should be processed
// It checks if the message has exceeded its TTL
// It checks if the message has reached maximum retry attempts
// It returns true if the message should be processed
func shouldProcessMessage(msg message, config Config) bool {
	// Check TTL before retrying based on event timestamp
	if isMessageExpired(msg, config) {
		return false
	}

	// Check if max retries reached
	if isMaxRetriesReached(msg, config) {
		return false
	}

	return true
}

// isMessageExpired checks if a message has exceeded its TTL
// It checks if the message has exceeded its TTL
// It logs the message as dropped due to TTL expiration
// It increments the error count
// It returns true if the message has exceeded its TTL
func isMessageExpired(msg message, config Config) bool {
	eventTime := time.Unix(0, msg.eventTimestamp)
	if time.Since(eventTime) > config.Processing.WriterRetryTTL {
		LogWarn("Dropping message %s from retry queue due to TTL expiration (event age: %v)",
			msg.uuid, time.Since(eventTime))
		GetMetricsManager().IncrementErrors()
		return true
	}
	return false
}

// isMaxRetriesReached checks if a message has reached maximum retry attempts
// It checks if the message has reached maximum retry attempts
// It logs the message as dropped due to maximum retry attempts
// It increments the error count
// It returns true if the message has reached maximum retry attempts
func isMaxRetriesReached(msg message, config Config) bool {
	if msg.retries >= config.Processing.WriterMaxRetries {
		LogError("Dropping message %s after %d retry attempts", msg.uuid, msg.retries)
		GetMetricsManager().IncrementErrors()
		return true
	}
	return false
}

// processRetryMessage handles the processing of a single retry message
// It tries to send the message
// It handles retry failure
// It logs the success of the retry attempt
func processRetryMessage(ctx context.Context, msg message, config Config) {
	// Try to send the message
	if err := retryMessageOnce(ctx, msg, config); err != nil {
		handleRetryFailure(msg, err)
	} else {
		LogDebug("Successfully retried message %s on attempt %d", msg.uuid, msg.retries+1)
	}
}

// handleRetryFailure handles the case when a retry attempt fails
// It increments the retry counter
// It re-queues the message for next round if not at max
// It logs the failure of the retry attempt
func handleRetryFailure(msg message, err error) {
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
}

// pauseAfterProcessing pauses after processing a message
// It logs the pause duration
// It waits for the pause duration or context cancellation
func pauseAfterProcessing(ctx context.Context, config Config) {
	LogDebug("Pausing %v before next retry attempt", config.Processing.WriterRetryPauseAfter)
	timer := time.NewTimer(config.Processing.WriterRetryPauseAfter)
	select {
	// BLOCKING: Wait for pause duration or context cancellation
	case <-timer.C:
		// Timer expired, continue with next message
	case <-ctx.Done():
		timer.Stop()
	}
}

// retryMessageOnce attempts to send a message once (no internal retries)
// It creates a context with timeout for retry
// It tries to add the message to Redis
// It returns an error if the message cannot be added to Redis
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
// It initializes the retry counter if not set
// It sends the message to the retry queue
// It returns true if the message is sent to the retry queue
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
