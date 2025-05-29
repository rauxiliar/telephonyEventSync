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
						timestampKey := "\"Event-Date-Timestamp\":\""
						if idx := strings.Index(eventStr, timestampKey); idx != -1 {
							start := idx + len(timestampKey)
							end := strings.Index(eventStr[start:], "\"")
							if end != -1 {
								timestampStr := eventStr[start : start+end]
								timestamp, err := strconv.ParseInt(timestampStr, 10, 64)
								if err == nil {
									eventTimestamp := time.Unix(0, timestamp*1000)
									readTime := time.Now()
									readLatency := readTime.Sub(eventTimestamp)
									if readLatency > 200 {
										log.Printf("[WARNING] High reader latency from event trigger detected for message %s: %v", msg.ID, readLatency)
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
						readTime:       time.Now(),
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
