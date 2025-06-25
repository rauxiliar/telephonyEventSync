package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

var latencyChan = make(chan latencyCheck, 10000)

func startLatencyChecker(config Config) {
	go func() {
		for check := range latencyChan {
			if check.timestamp <= 0 {
				continue
			}

			now := time.Now()
			eventTime := time.Unix(0, check.timestamp)
			totalLatency := now.Sub(eventTime)
			ObserveTotalLatency(float64(totalLatency.Milliseconds()))
			LogLatency("total", totalLatency, config.Processing.TotalMaxLatency, map[string]any{
				"uuid":       check.uuid,
				"event_type": check.eventType,
				"event_time": eventTime.Format(time.RFC3339),
				"check_time": now.Format(time.RFC3339),
			})
		}
	}()
}

func processPipeline(ctx context.Context, pipe redis.Pipeliner, pendingMsgs []message, workerID int, config Config) {
	// Use configurable timeout for pipeline execution
	pipeCtx, cancel := context.WithTimeout(ctx, config.GetRedisRemoteWriteTimeout())
	defer cancel()

	// Execute pipeline
	cmds, err := pipe.Exec(pipeCtx)

	if err != nil {
		LogError("Pipeline execution failed for worker %d: %v", workerID, err)
		GetMetricsManager().IncrementErrors()

		// Send all messages to retry queue asynchronously
		for _, msg := range pendingMsgs {
			sendToRetryQueue(msg)
		}
		return
	}

	// Track successful messages and process them
	var processedCount int64
	var errorCount int64

	for i, cmd := range cmds {
		if cmd.Err() == nil {
			msg := pendingMsgs[i]

			// Calculate latency only for successful messages
			writerTime := time.Now()
			writerLatency := writerTime.Sub(msg.readTime)
			ObserveWriterLatency(float64(writerLatency.Milliseconds()))
			LogLatency("writer", writerLatency, config.Processing.WriterMaxLatency, map[string]any{
				"uuid":       msg.uuid,
				"event_type": msg.eventType,
				"read_time":  msg.readTime.Format(time.RFC3339),
				"write_time": writerTime.Format(time.RFC3339),
			})

			// Send to latency channel only for valid timestamps
			if msg.eventTimestamp > 0 {
				select {
				case latencyChan <- latencyCheck{
					uuid:      msg.uuid,
					timestamp: msg.eventTimestamp,
					eventType: msg.eventType,
				}:
				default:
					LogWarn("Latency channel full, message %s discarded", msg.uuid)
				}
			}

			processedCount++
		} else {
			LogError("Failed to add message to stream: %v", cmd.Err())
			// The message is not added to the stream, we need to retry it
			if !sendToRetryQueue(pendingMsgs[i]) {
				errorCount++
			}
		}
	}

	// Update metrics in batch
	GetMetricsManager().UpdateBatchMetrics(processedCount, errorCount)
}

func writer(ctx context.Context, ch <-chan message, wg *sync.WaitGroup, workerID int, config Config) {
	defer wg.Done()

	// Panic recovery for the main goroutine
	defer PanicRecoveryFunc(fmt.Sprintf("writer worker %d", workerID))()

	pipelineTimeout := config.Processing.WriterPipelineTimeout
	batchSize := config.Processing.WriterBatchSize

	eventsMaxLen := config.Streams.Events.MaxLen
	jobsMaxLen := config.Streams.Jobs.MaxLen
	jobsStreamName := config.Streams.Jobs.Name

	// Use configurable timeout for pipeline operations
	redisTimeout := config.GetRedisRemoteWriteTimeout()

	pipe := rRemote.Pipeline()
	lastPipelineExec := time.Now()
	pendingMsgs := make([]message, 0, batchSize)

	for {
		select {
		case msg, ok := <-ch:
			if !ok {
				if pipe.Len() > 0 {
					processPipeline(ctx, pipe, pendingMsgs, workerID, config)
				}
				return
			}

			maxLen := eventsMaxLen
			if msg.stream == jobsStreamName {
				maxLen = jobsMaxLen
			}

			// Create context with timeout for this operation
			pipeCtx, cancel := context.WithTimeout(ctx, redisTimeout)
			pipe.XAdd(pipeCtx, &redis.XAddArgs{
				Stream: msg.stream,
				Values: msg.values,
				MaxLen: maxLen,
				Approx: true,
			})
			cancel()

			pendingMsgs = append(pendingMsgs, msg)

			// Execute pipeline if batch is full or timeout reached
			if pipe.Len() >= batchSize || time.Since(lastPipelineExec) > pipelineTimeout {
				processPipeline(ctx, pipe, pendingMsgs, workerID, config)
				pendingMsgs = pendingMsgs[:0] // Clear slice but keep capacity
				lastPipelineExec = time.Now()
				pipe = rRemote.Pipeline() // Create new pipeline after execution
			}
		default:
			if pipe.Len() > 0 {
				processPipeline(ctx, pipe, pendingMsgs, workerID, config)
				pendingMsgs = pendingMsgs[:0] // Clear slice but keep capacity
				lastPipelineExec = time.Now()
				pipe = rRemote.Pipeline() // Create new pipeline after execution
			}

			select {
			case <-time.After(100 * time.Microsecond): // Much shorter wait
			case <-ctx.Done():
				return
			}
		}
	}
}
