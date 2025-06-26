package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

var latencyChan = make(chan latencyCheck, 10000)

// writerState holds the state for a writer worker
type writerState struct {
	pipelineTimeout  time.Duration
	batchSize        int
	eventsMaxLen     int64
	jobsMaxLen       int64
	jobsStreamName   string
	redisTimeout     time.Duration
	pipe             redis.Pipeliner
	lastPipelineExec time.Time
	pendingMsgs      []message
}

// startLatencyChecker processes latency checks from the latency channel
// This function runs the latency checker worker loop
// It processes latency checks from the latency channel
// It processes each latency check
// It returns an error if the latency check fails
func startLatencyChecker(config Config) {
	for check := range latencyChan {
		processLatencyCheck(check, config)
	}
}

// processLatencyCheck processes a single latency check
// It calculates the total latency
// It logs the latency
// It returns an error if the latency check fails
func processLatencyCheck(check latencyCheck, config Config) {
	if check.timestamp <= 0 {
		return
	}

	eventTime := time.Unix(0, check.timestamp)
	now := time.Now()
	totalLatency := now.Sub(eventTime)

	promTotalLatency.Observe(float64(totalLatency.Milliseconds()))

	LogLatency("total", totalLatency, config.Processing.TotalMaxLatency, map[string]any{
		"uuid":       check.uuid,
		"event_type": check.eventType,
		"event_time": eventTime.Format(time.RFC3339),
		"check_time": now.Format(time.RFC3339),
	})
}

// processPipeline processes a pipeline
// It uses a configurable timeout for pipeline execution
// It executes the pipeline
// It handles pipeline execution errors
// It processes the pipeline results
func processPipeline(ctx context.Context, pipe redis.Pipeliner, pendingMsgs []message, workerID int, config Config) {
	// Use configurable timeout for pipeline execution
	pipeCtx, cancel := context.WithTimeout(ctx, config.GetRedisRemoteWriteTimeout())
	defer cancel()

	// Execute pipeline
	cmds, err := pipe.Exec(pipeCtx)

	if err != nil {
		handlePipelineError(err, pendingMsgs, workerID)
		return
	}

	processPipelineResults(cmds, pendingMsgs, config)
}

// handlePipelineError handles pipeline execution errors
// It logs the error
// It increments the error count
// It sends all messages to retry queue asynchronously
func handlePipelineError(err error, pendingMsgs []message, workerID int) {
	LogError("Pipeline execution failed for worker %d: %v", workerID, err)
	GetMetricsManager().IncrementErrors()

	// Send all messages to retry queue asynchronously
	for _, msg := range pendingMsgs {
		sendToRetryQueue(msg)
	}
}

// processPipelineResults processes the results of pipeline execution
// It processes successful messages
// It processes failed messages
// It updates the metrics in batch
func processPipelineResults(cmds []redis.Cmder, pendingMsgs []message, config Config) {
	var processedCount int64
	var errorCount int64

	for i, cmd := range cmds {
		if cmd.Err() == nil {
			processSuccessfulMessage(pendingMsgs[i], config)
			processedCount++
		} else {
			errorCount += processFailedMessage(pendingMsgs[i], cmd.Err())
		}
	}

	// Update metrics in batch
	GetMetricsManager().UpdateBatchMetrics(processedCount, errorCount)
}

// processSuccessfulMessage handles a successfully processed message
// It calculates the writer latency
// It logs the latency
// It sends the message to the latency channel
func processSuccessfulMessage(msg message, config Config) {
	// Calculate latency only for successful messages
	writerTime := time.Now()
	writerLatency := writerTime.Sub(msg.readTime)

	promWriterLatency.Observe(float64(writerLatency.Milliseconds()))
	LogLatency("writer", writerLatency, config.Processing.WriterMaxLatency, map[string]any{
		"uuid":       msg.uuid,
		"event_type": msg.eventType,
		"read_time":  msg.readTime.Format(time.RFC3339),
		"write_time": writerTime.Format(time.RFC3339),
	})

	sendToLatencyChannel(msg)
}

// sendToLatencyChannel sends message to latency channel for total latency calculation
// It sends the message to the latency channel
func sendToLatencyChannel(msg message) {
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
}

// processFailedMessage handles a failed message and returns error count
// It logs the error
// It sends the message to retry queue
// It returns the error count
func processFailedMessage(msg message, err error) int64 {
	LogError("Failed to add message to stream: %v", err)
	// The message is not added to the stream, we need to retry it
	if !sendToRetryQueue(msg) {
		return 1
	}
	return 0
}

// executePipelineAndReset executes the pipeline and resets the writer state
// It processes the pipeline
// It resets the writer state
func executePipelineAndReset(ctx context.Context, pipe *redis.Pipeliner, pendingMsgs *[]message, lastPipelineExec *time.Time, workerID int, config Config) {
	processPipeline(ctx, *pipe, *pendingMsgs, workerID, config)
	resetWriterState(pipe, pendingMsgs, lastPipelineExec)
}

// resetWriterState resets the writer state after pipeline execution
// It clears the pending messages
// It creates a new pipeline
func resetWriterState(pipe *redis.Pipeliner, pendingMsgs *[]message, lastPipelineExec *time.Time) {
	*pendingMsgs = (*pendingMsgs)[:0] // Clear slice but keep capacity
	*lastPipelineExec = time.Now()
	*pipe = rRemote.Pipeline() // Create new pipeline after execution
}

// writer is the main writer function
// It initializes the writer state
// It processes messages from the channel
// It handles channel closed state
// It handles idle state
func writer(ctx context.Context, ch <-chan message, wg *sync.WaitGroup, workerID int, config Config) {
	defer wg.Done()

	// Panic recovery for the main goroutine
	defer PanicRecoveryFunc(fmt.Sprintf("writer worker %d", workerID))()

	writerState := initializeWriterState(config)

	for {
		// BLOCKING: Waits for messages from channel or idle state
		select {
		case msg, ok := <-ch:
			if !ok {
				handleChannelClosed(ctx, &writerState, workerID, config)
				return
			}
			processMessage(ctx, msg, &writerState, config)
		default:
			handleIdleState(ctx, &writerState, workerID, config)
		}
	}
}

// initializeWriterState initializes the writer state with configuration
// It creates a new writer state
// It returns the writer state
func initializeWriterState(config Config) writerState {
	return writerState{
		pipelineTimeout:  config.Processing.WriterPipelineTimeout,
		batchSize:        config.Processing.WriterBatchSize,
		eventsMaxLen:     config.Streams.Events.MaxLen,
		jobsMaxLen:       config.Streams.Jobs.MaxLen,
		jobsStreamName:   config.Streams.Jobs.Name,
		redisTimeout:     config.GetRedisRemoteWriteTimeout(),
		pipe:             rRemote.Pipeline(),
		lastPipelineExec: time.Now(),
		pendingMsgs:      make([]message, 0, config.Processing.WriterBatchSize),
	}
}

// handleChannelClosed handles the case when the input channel is closed
// It executes the pipeline and resets the writer state
func handleChannelClosed(ctx context.Context, state *writerState, workerID int, config Config) {
	if state.pipe.Len() > 0 {
		executePipelineAndReset(ctx, &state.pipe, &state.pendingMsgs, &state.lastPipelineExec, workerID, config)
	}
}

// processMessage processes a single message
// It adds the message to the pipeline
// It executes the pipeline if batch is full or timeout reached
func processMessage(ctx context.Context, msg message, state *writerState, config Config) {
	maxLen := getMaxLenForStream(msg, state)
	addMessageToPipeline(ctx, msg, maxLen, state.redisTimeout, state.pipe)
	state.pendingMsgs = append(state.pendingMsgs, msg)

	// Execute pipeline if batch is full or timeout reached
	if state.pipe.Len() >= state.batchSize || time.Since(state.lastPipelineExec) > state.pipelineTimeout {
		executePipelineAndReset(ctx, &state.pipe, &state.pendingMsgs, &state.lastPipelineExec, 0, config)
	}
}

// getMaxLenForStream returns the appropriate max length for the message stream
// It returns the appropriate max length for the message stream
func getMaxLenForStream(msg message, state *writerState) int64 {
	if msg.stream == state.jobsStreamName {
		return state.jobsMaxLen
	}
	return state.eventsMaxLen
}

// addMessageToPipeline adds a message to the Redis pipeline
// It creates a context with timeout for this operation
// It adds the message to the pipeline
func addMessageToPipeline(ctx context.Context, msg message, maxLen int64, redisTimeout time.Duration, pipe redis.Pipeliner) {
	// Create context with timeout for this operation
	pipeCtx, cancel := context.WithTimeout(ctx, redisTimeout)
	pipe.XAdd(pipeCtx, &redis.XAddArgs{
		Stream: msg.stream,
		Values: msg.values,
		MaxLen: maxLen,
		Approx: true,
	})
	cancel()
}

// handleIdleState handles the idle state when no messages are available
// It executes the pipeline and resets the writer state
// It waits for 100 microseconds to avoid busy waiting when idle
// It waits for context cancellation
func handleIdleState(ctx context.Context, state *writerState, workerID int, config Config) {
	if state.pipe.Len() > 0 {
		executePipelineAndReset(ctx, &state.pipe, &state.pendingMsgs, &state.lastPipelineExec, workerID, config)
	}

	select {
	// BLOCKING: Wait 100 microseconds to avoid busy waiting when idle
	case <-time.After(100 * time.Microsecond): // Much shorter wait
	case <-ctx.Done():
	}
}
