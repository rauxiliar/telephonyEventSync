package main

import (
	"context"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

type latencyCheck struct {
	id        string
	timestamp int64
}

var latencyChan = make(chan latencyCheck, 1000000)

func startLatencyChecker(config Config) {
	go func() {
		for check := range latencyChan {
			if check.timestamp <= 0 {
				continue
			}

			now := time.Now()
			eventTime := time.Unix(0, check.timestamp)
			totalLatency := now.Sub(eventTime)

			if totalLatency > config.Processing.TotalMaxLatency {
				LogWarn("High total latency detected since event trigger until writer processing for message %s: %v", check.id, totalLatency)
			}
		}
	}()
}

func processPipeline(ctx context.Context, pipe redis.Pipeliner, pendingMsgs []message, workerID int, config Config) {
	// Execute pipeline
	cmds, err := pipe.Exec(ctx)
	if err != nil {
		LogError("Pipeline execution failed for worker %d: %v", workerID, err)
		metrics.Lock()
		metrics.errors++
		metrics.Unlock()
		return
	}

	// Track successful messages and process them
	var processedCount int64
	var errorCount int64

	for i, cmd := range cmds {
		if cmd.Err() == nil {
			msg := pendingMsgs[i]

			// Calculate latency
			writerTime := time.Now()
			writerLatency := writerTime.Sub(msg.readTime)
			if writerLatency > config.Processing.WriterMaxLatency {
				LogWarn("High writer latency after reader processing detected for message %s: %v", msg.id, writerLatency)
			}

			// Send to latency channel
			select {
			case latencyChan <- latencyCheck{
				id:        msg.id,
				timestamp: msg.eventTimestamp,
			}:
			default:
				LogWarn("Latency channel full, message %s discarded", msg.id)
			}

			// ACK the message only if it came from Redis reader
			if config.Reader.Type == "redis" && msg.id != "" {
				if err := rLocal.XAck(ctx, msg.stream, config.Redis.Group, msg.id).Err(); err != nil {
					LogError("Failed to acknowledge message %s: %v", msg.id, err)
					errorCount++
				}
			}

			processedCount++
		} else {
			LogError("Failed to add message to stream: %v", cmd.Err())
			errorCount++
		}
	}

	// Update metrics in batch
	metrics.Lock()
	metrics.messagesProcessed += processedCount
	metrics.errors += errorCount
	metrics.lastSyncTime = time.Now()
	metrics.Unlock()
}

func writer(ctx context.Context, ch <-chan message, wg *sync.WaitGroup, workerID int, config Config) {
	defer wg.Done()

	pipelineTimeout := config.Processing.WriterPipelineTimeout
	batchSize := config.Processing.WriterBatchSize

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

			// Add message to pipeline
			pipe.XAdd(ctx, &redis.XAddArgs{
				Stream: msg.stream,
				Values: msg.values,
			})
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
			time.Sleep(10 * time.Microsecond)
		}
	}
}
