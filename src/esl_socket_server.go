package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"maps"

	"github.com/0x19/goesl"
	"github.com/op/go-logging"
)

// Event types that should be published to background-jobs stream
var eslEventsToPublish = map[string]bool{
	"BACKGROUND_JOB":           true, // Will be filtered by Event-Calling-Function
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

	// Configure goesl logging to only show errors
	logging.SetLevel(logging.ERROR, "goesl")

	// Create ESL client
	client, err := goesl.NewClient(config.ESL.Host, uint(config.ESL.Port), config.ESL.Password, 20)
	if err != nil {
		LogError("Failed to create ESL client: %v", err)
		return
	}
	defer client.Close()

	// Start handling messages in a goroutine
	go client.Handle()

	// Build event list from our maps
	var events []string
	for event := range eslEventsToPublish {
		events = append(events, event)
	}
	for event := range eslEventsToPush {
		events = append(events, event)
	}

	// Subscribe to specific events
	eventCmd := fmt.Sprintf("events json %s", strings.Join(events, " "))
	if err := client.Send(eventCmd); err != nil {
		LogError("Failed to subscribe to events: %v", err)
		return
	}

	LogInfo("ESL server started and connected to FreeSWITCH at %s:%d", config.ESL.Host, config.ESL.Port)

	// Create worker pool
	workerCount := 10
	eventChan := make(chan *goesl.Message, workerCount*2)

	// Start workers
	for i := 0; i < workerCount; i++ {
		go func() {
			for evt := range eventChan {
				processESLEvent(evt, ch, config)
			}
		}()
	}

	// Process events
	for {
		select {
		case <-ctx.Done():
			close(eventChan)
			LogInfo("Stopping ESL server")
			return
		default:
			// Read event with timeout
			readChan := make(chan *goesl.Message, 1)
			errChan := make(chan error, 1)

			go func() {
				evt, err := client.ReadMessage()
				if err != nil {
					errChan <- err
					return
				}
				readChan <- evt
			}()

			select {
			case err := <-errChan:
				if !isClosedError(err) {
					LogError("Error reading ESL event: %v (connection to %s:%d)", err, config.ESL.Host, config.ESL.Port)
				}
				continue
			case evt := <-readChan:
				// Send to worker pool
				select {
				case eventChan <- evt:
					// Event sent to worker pool
				default:
					LogWarn("Worker pool is full, dropping event")
				}
			case <-time.After(1 * time.Second): // Reduced timeout
				LogWarn("Timeout reading from ESL socket")
				continue
			}
		}
	}
}

func processESLEvent(evt *goesl.Message, ch chan<- message, config Config) {
	if evt == nil {
		LogError("Received nil event")
		return
	}

	// Ignore command replies
	if evt.GetHeader("Content-Type") == "command/reply" {
		return
	}

	// Get event type directly from headers
	eventType := evt.GetHeader("Event-Name")
	if eventType == "" {
		LogError("Event type not found in headers. Headers: %+v", evt.Headers)
		return
	}

	// Extract event timestamp
	var eventTimestamp int64
	if timestamp := evt.GetHeader("Event-Date-Timestamp"); timestamp != "" {
		if ts, err := strconv.ParseInt(timestamp, 10, 64); err == nil {
			eventTimestamp = ts * 1000 // Convert to nanoseconds
		}
	}

	// Check reader latency
	readTime := time.Now()
	eventTime := time.Unix(0, eventTimestamp)
	readerLatency := readTime.Sub(eventTime)
	if readerLatency > config.Processing.ReaderMaxLatency {
		LogWarn("High reader latency since event trigger detected for Unix socket message: %v", readerLatency)
	}

	// Determine stream based on event type
	var stream string
	if eslEventsToPublish[eventType] {
		// For BACKGROUND_JOB, check Event-Calling-Function
		if eventType == "BACKGROUND_JOB" {
			if callingFunction := evt.GetHeader("Event-Calling-Function"); callingFunction != "api_exec" {
				return
			}
		}
		stream = config.Streams.Jobs.Name
	} else if eslEventsToPush[eventType] {
		stream = config.Streams.Events.Name
	} else {
		return
	}

	// Convert all headers to a map
	eventHeaders := make(map[string]string)
	maps.Copy(eventHeaders, evt.Headers)

	// Convert event to JSON
	eventJSON, err := json.Marshal(eventHeaders)
	if err != nil {
		LogError("Error serializing event: %v", err)
		return
	}

	// Create message
	msg := message{
		stream:         stream,
		values:         map[string]string{"event": string(eventJSON)},
		readTime:       readTime,
		eventTimestamp: eventTimestamp,
	}

	// Send message to channel with shorter timeout
	select {
	case ch <- msg:
		// Message sent successfully
	case <-time.After(100 * time.Millisecond):
		LogWarn("Timeout sending message to channel %s, buffer might be full", stream)
		// Try one more time without timeout
		select {
		case ch <- msg:
			// Message sent after retry
		default:
			LogError("Failed to send message to channel %s, buffer is full", stream)
		}
	}
}
