package main

import (
	"bytes"
	"context"
	"strconv"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

var stopChan = make(chan struct{})

func reader(ctx context.Context, ch chan<- message, wg *sync.WaitGroup, id int, config Config) {
	defer wg.Done()

	// Pre-allocate streams slice with exact size
	streams := make([]string, 4) // 2 streams * 2 (stream + ">")
	streams[0] = config.Streams.Events
	streams[1] = config.Streams.Jobs
	streams[2] = ">"
	streams[3] = ">"

	// Batch metrics update
	metricsUpdateTicker := time.NewTicker(1 * time.Second)
	defer metricsUpdateTicker.Stop()

	// Pre-allocate timestamp key for faster string operations
	timestampKey := []byte("\"Event-Date-Timestamp\":\"")

	for {
		select {
		case <-stopChan:
			LogInfo("Stopping reader %d after cleanup", id)
			return
		case <-metricsUpdateTicker.C:
			metrics.Lock()
			metrics.queueSize = len(ch)
			metrics.Unlock()
		default:
			res, err := rLocal.XReadGroup(ctx, &redis.XReadGroupArgs{
				Group:    config.Redis.Group,
				Consumer: config.Redis.Consumer,
				Streams:  streams,
				Count:    config.Processing.ReaderBatchSize,
				Block:    config.Processing.ReaderBlockTime,
				NoAck:    true,
			}).Result()

			if err != nil {
				if err == redis.Nil {
					continue
				}
				LogError("Reader %d error: %v", id, err)
				continue
			}

			for _, streamRes := range res {
				for _, msg := range streamRes.Messages {
					eventStr, _ := msg.Values["event"].(string)

					// Extract event timestamp if available
					var eventTimestamp int64
					if eventStr != "" {
						// Use bytes.Index for faster string search
						if idx := bytes.Index([]byte(eventStr), timestampKey); idx != -1 {
							start := idx + len(timestampKey)
							end := bytes.Index([]byte(eventStr[start:]), []byte("\""))
							if end != -1 {
								timestampStr := eventStr[start : start+end]
								timestamp, err := strconv.ParseInt(timestampStr, 10, 64)
								if err == nil {
									eventTimestamp = timestamp * 1000 // Convert to nanoseconds
									readTime := time.Now()
									eventTime := time.Unix(0, eventTimestamp)
									readerLatency := readTime.Sub(eventTime)
									if readerLatency > config.Processing.ReaderMaxLatency {
										LogWarn("High reader latency since event trigger detected for message %s: %v", msg.ID, readerLatency)
									}
								}
							}
						}
					}

					select {
					case ch <- message{
						stream:         streamRes.Stream,
						id:             msg.ID,
						values:         map[string]string{"event": eventStr},
						readTime:       time.Now(),
						eventTimestamp: eventTimestamp,
					}:
					case <-stopChan:
						return
					default:
						LogWarn("Buffer full, message discarded: %s", msg.ID)
					}
				}
			}
		}
	}
}
