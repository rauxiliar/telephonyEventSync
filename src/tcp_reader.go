package main

import (
	"bytes"
	"context"
	"net"
	"strconv"
	"sync"
	"time"
)

func tcpReader(ctx context.Context, ch chan<- message, wg *sync.WaitGroup, id int, config Config) {
	defer wg.Done()

	// Connect to TCP loopback
	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", config.TCP.Address)
	if err != nil {
		LogError("Error connecting to TCP loopback: %v", err)
		return
	}
	defer conn.Close()

	// Buffer for reading
	buffer := make([]byte, config.TCP.BufferSize)

	// Batch metrics update
	metricsUpdateTicker := time.NewTicker(1 * time.Second)
	defer metricsUpdateTicker.Stop()

	// Pre-allocate timestamp key for faster string operations
	timestampKey := []byte("\"Event-Date-Timestamp\":\"")

	for {
		select {
		case <-stopChan:
			LogInfo("Stopping TCP reader %d after cleanup", id)
			return
		case <-metricsUpdateTicker.C:
			metrics.Lock()
			metrics.queueSize = len(ch)
			metrics.Unlock()
		default:
			// Read data from socket
			n, err := conn.Read(buffer)
			if err != nil {
				LogError("Error reading from TCP: %v", err)
				// Try to reconnect after a brief delay
				time.Sleep(time.Second)
				conn, err = d.DialContext(ctx, "tcp", config.TCP.Address)
				if err != nil {
					LogError("Error reconnecting to TCP: %v", err)
					continue
				}
				continue
			}

			if n > 0 {
				// Convert read data to string
				eventStr := string(buffer[:n])

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
									LogWarn("High reader (TCP) latency since event trigger detected: %v", readerLatency)
								}
							}
						}
					}
				}

				// Create message with the same format as Redis reader
				select {
				case ch <- message{
					stream:         "tcp_socket",
					id:             time.Now().String(), // Use timestamp as ID
					values:         map[string]string{"event": eventStr},
					readTime:       time.Now(),
					eventTimestamp: eventTimestamp,
				}:
				case <-stopChan:
					return
				default:
					LogWarn("Buffer full, message discarded from TCP")
				}
			}
		}
	}
}
