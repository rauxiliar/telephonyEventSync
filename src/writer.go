package main

import (
	"context"
	"log"
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
			eventTime := time.Unix(0, check.timestamp*1000)
			totalLatency := now.Sub(eventTime)

			if totalLatency > config.Processing.TotalMaxLatency {
				log.Printf("[WARNING] High latency detected for message %s: %v", check.id, totalLatency)
			}
		}
	}()
}

func processPipeline(ctx context.Context, pipe redis.Pipeliner, pendingMsgs []message, workerID int, config Config) {
	start := time.Now()

	// Execute pipeline
	cmds, err := pipe.Exec(ctx)
	if err != nil {
		log.Printf("[WRITER ERROR - PIPELINE] Worker %d: %v\n", workerID, err)
		metrics.Lock()
		metrics.errors++
		metrics.Unlock()
		return
	}

	// Track successful messages and process them
	for i, cmd := range cmds {
		if cmd.Err() == nil {
			msg := pendingMsgs[i]

			// Calculate latency if event timestamp is available
			if msg.eventTimestamp > 0 {
				eventTime := time.Unix(0, msg.eventTimestamp*1000)
				writerTime := time.Now()
				writerLatency := writerTime.Sub(eventTime)
				if writerLatency > config.Processing.TotalMaxLatency {
					log.Printf("[WARNING] High writer latency detected for message %s: %v", msg.id, writerLatency)
				}

				// Send to latency channel
				select {
				case latencyChan <- latencyCheck{
					id:        msg.id,
					timestamp: msg.eventTimestamp,
				}:
				default:
					log.Printf("[WARNING] Latency channel full, message %s discarded", msg.id)
				}
			}

			// ACK the message
			if err := rLocal.XAck(ctx, msg.stream, config.Redis.Group, msg.id).Err(); err != nil {
				log.Printf("[WRITER ERROR - XACK] Worker %d: %v\n", workerID, err)
				metrics.Lock()
				metrics.errors++
				metrics.Unlock()
			}

			metrics.Lock()
			metrics.messagesProcessed++
			metrics.Unlock()
		} else {
			log.Printf("[WRITER ERROR - XADD] Worker %d: %v\n", workerID, cmd.Err())
			metrics.Lock()
			metrics.errors++
			metrics.Unlock()
		}
	}

	latency := time.Since(start)
	metrics.Lock()
	metrics.latencyWrite = latency
	metrics.Unlock()

	if latency > config.Processing.WriterMaxLatency {
		log.Printf("[WARNING] High writer processing latency detected: %v", latency)
	}
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
