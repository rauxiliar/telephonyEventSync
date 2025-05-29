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

func reader(ctx context.Context, ch chan<- message, wg *sync.WaitGroup, id int, config Config) {
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
			log.Printf("[READER %d] Stopping reader after cleanup", id)
			return
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
				log.Printf("[READER %d] Error: %v\n", id, err)
				time.Sleep(50 * time.Millisecond)
				continue
			}

			metrics.Lock()
			metrics.queueSize = len(ch)
			metrics.Unlock()

			for _, streamRes := range res {
				for _, msg := range streamRes.Messages {
					eventStr, _ := msg.Values["event"].(string)
					values := map[string]string{"event": eventStr}

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
									eventTimestamp = timestamp * 1000 // Convert to nanoseconds
									readTime := time.Now()
									eventTime := time.Unix(0, eventTimestamp)
									readerLatency := readTime.Sub(eventTime)
									if readerLatency > config.Processing.ReaderMaxLatency {
										log.Printf("[READER %d] High reader latency since event trigger detected for message %s: %v", id, msg.ID, readerLatency)
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
						log.Printf("[READER %d] Buffer full, message discarded: %s", id, msg.ID)
					}
				}
			}
		}
	}
}
