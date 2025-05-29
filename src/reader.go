package main

import (
	"context"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

var stopChan = make(chan struct{})

func reader(ctx context.Context, ch chan<- message, wg *sync.WaitGroup, config Config) {
	defer wg.Done()

	// Pre-allocate streams slice with exact size
	streams := make([]string, len(config.Streams)*2)
	for i, stream := range config.Streams {
		streams[i] = stream
		streams[i+len(config.Streams)] = ">"
	}

	for {
		select {
		case <-stopChan:
			log.Printf("[READER] Stopping reader after cleanup")
			return
		default:
			start := time.Now()
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
				log.Printf("[READER ERROR] %v\n", err)
				time.Sleep(50 * time.Millisecond)
				continue
			}

			metrics.Lock()
			metrics.queueSize = len(ch)
			metrics.Unlock()

			for _, streamRes := range res {
				for _, msg := range streamRes.Messages {
					// Create new map for each message
					values := make(map[string]string, config.Processing.ReaderBatchSize)
					for k, v := range msg.Values {
						if str, ok := v.(string); ok {
							values[k] = str
						}
					}

					// Extract event timestamp if available
					var eventTimestamp int64
					if eventStr, ok := values["event"]; ok {
						if idx := strings.Index(eventStr, "\"Event-Date-Timestamp\":\""); idx != -1 {
							start := idx + 22
							end := strings.Index(eventStr[start:], "\"")
							if end != -1 {
								timestampStr := eventStr[start : start+end]
								if ts, err := strconv.ParseInt(timestampStr, 10, 64); err == nil {
									eventTimestamp = ts
									eventTime := time.Unix(0, ts*1000)
									readTime := time.Now()
									readLatency := readTime.Sub(eventTime)
									if readLatency > config.Processing.TotalMaxLatency {
										log.Printf("[WARNING] High reader latency detected for message %s: %v", msg.ID, readLatency)
									}
								}
							}
						}
					}

					select {
					case ch <- message{
						stream:         streamRes.Stream,
						id:             msg.ID,
						values:         values,
						timestamp:      time.Now(),
						eventTimestamp: eventTimestamp,
					}:
					case <-stopChan:
						return
					default:
						log.Printf("[WARNING] Buffer full, message discarded: %s", msg.ID)
					}
				}
			}

			latency := time.Since(start)
			metrics.Lock()
			metrics.latency = latency
			metrics.Unlock()

			if latency > config.Processing.ReaderMaxLatency {
				log.Printf("[WARNING] High reader processing latency detected: %v", latency)
			}
		}
	}
}
