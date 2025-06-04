package main

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/0x19/goesl"
	"github.com/op/go-logging"
)

// Event types that should be published to background-jobs stream
var eslEventsToPublish = map[string]bool{
	"BACKGROUND_JOB":           true,
	"CHANNEL_EXECUTE":          true,
	"CHANNEL_EXECUTE_COMPLETE": true,
	"DTMF":                     true,
	"DETECTED_SPEECH":          true,
}

// Event types that should be pushed to events stream
var eslEventsToPush = map[string]bool{
	"CHANNEL_ANSWER": true,
	"CHANNEL_HANGUP": true,
	"DTMF":           true,
	"CUSTOM":         true,
}

func eslSocketServer(ctx context.Context, ch chan<- message, wg *sync.WaitGroup, config Config) {
	defer wg.Done()

	logging.SetLevel(logging.ERROR, "goesl")

	client, err := goesl.NewClient(config.ESL.Host, uint(config.ESL.Port), config.ESL.Password, 100)
	if err != nil {
		LogError("Failed to create ESL client: %v", err)
		return
	}
	defer client.Close()

	go client.Handle()

	var events []string
	for event := range eslEventsToPublish {
		events = append(events, event)
	}
	for event := range eslEventsToPush {
		events = append(events, event)
	}

	eventCmd := fmt.Sprintf("events json %s", strings.Join(events, " "))
	if err := client.Send(eventCmd); err != nil {
		LogError("Failed to subscribe to events: %v", err)
		return
	}

	LogInfo("ESL server started and connected to FreeSWITCH at %s:%d", config.ESL.Host, config.ESL.Port)

	// Buffered channel to avoid blocking reads
	eslEventsChan := make(chan *goesl.Message, config.Processing.BufferSize)

	// Goroutine to read messages and push into buffered channel
	go func() {
		for {
			select {
			case <-ctx.Done():
				LogInfo("Stopping ESL reader loop")
				return
			default:
				evt, err := client.ReadMessage()
				if err != nil {
					if !isClosedError(err) {
						LogError("Error reading ESL event: %v (connection to %s:%d)", err, config.ESL.Host, config.ESL.Port)
					}
					continue
				}
				select {
				case eslEventsChan <- evt:
				default:
					LogWarn("ESL events channel full, event dropped")
				}
			}
		}
	}()

	// Worker pool to process events in parallel
	workerCount := config.Processing.ReaderWorkers
	var workerWg sync.WaitGroup
	workerWg.Add(workerCount)

	for range workerCount {
		go func() {
			defer workerWg.Done()
			for evt := range eslEventsChan {
				processESLEvent(evt, ch, config)
			}
		}()
	}

	// Wait for context cancellation
	<-ctx.Done()
	LogInfo("Stopping ESL server")
	close(eslEventsChan)
	workerWg.Wait()
}

func processESLEvent(evt *goesl.Message, ch chan<- message, config Config) {
	if evt.GetHeader("Content-Type") == "command/reply" {
		return
	}

	eventType := evt.GetHeader("Event-Name")
	if eventType == "" {
		LogError("Event type not found in headers. Headers: %+v", evt)
		return
	}

	var eventTimestamp int64
	if timestamp := evt.GetHeader("Event-Date-Timestamp"); timestamp != "" {
		if ts, err := strconv.ParseInt(timestamp, 10, 64); err == nil {
			eventTimestamp = ts * 1000 // nanoseconds
		}
	}

	readTime := time.Now()
	eventTime := time.Unix(0, eventTimestamp)
	readerLatency := readTime.Sub(eventTime)
	if readerLatency > config.Processing.ReaderMaxLatency {
		LogWarn("High reader latency since event trigger detected for ESL socket message: %v", readerLatency)
	}

	var stream string
	if eslEventsToPublish[eventType] {
		if eventType == "BACKGROUND_JOB" {
			if evt.GetHeader("Event-Calling-Function") != "api_exec" {
				return
			}
		}
		stream = config.Streams.Jobs.Name
	} else if eslEventsToPush[eventType] {
		stream = config.Streams.Events.Name
	} else {
		return
	}

	msg := message{
		stream:         stream,
		values:         map[string]string{"event": fmt.Sprintf("%v", evt)},
		readTime:       readTime,
		eventTimestamp: eventTimestamp,
	}

	select {
	case ch <- msg:
	case <-time.After(10 * time.Millisecond):
		select {
		case ch <- msg:
		default:
			LogError("Failed to send message to channel %s, buffer is full", stream)
		}
	}
}
