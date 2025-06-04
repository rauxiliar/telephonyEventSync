package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/0x19/goesl"
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
	eventCmd := fmt.Sprintf("event plain %s", strings.Join(events, " "))
	if err := client.Send(eventCmd); err != nil {
		LogError("Failed to subscribe to events: %v", err)
		return
	}

	LogInfo("ESL server started and connected to FreeSWITCH")

	// Process events
	for {
		select {
		case <-ctx.Done():
			LogInfo("Stopping ESL server")
			return
		default:
			// Read event
			evt, err := client.ReadMessage()
			if err != nil {
				if !isClosedError(err) {
					LogError("Error reading ESL event: %v", err)
				}
				continue
			}

			// Process event
			processESLEvent(evt, ch, config)
		}
	}
}

func processESLEvent(evt *goesl.Message, ch chan<- message, config Config) {
	if evt == nil {
		LogError("Received nil event")
		return
	}

	// Check if this is an event message
	contentType := evt.GetHeader("Content-Type")
	if contentType != "text/event-plain" {
		return
	}

	// Get event body
	body := string(evt.Body)
	if body == "" {
		LogError("Empty event body")
		return
	}

	// Parse event body into headers
	eventHeaders := make(map[string]string)
	for _, line := range strings.Split(body, "\n") {
		if line == "" {
			continue
		}
		parts := strings.SplitN(line, ": ", 2)
		if len(parts) == 2 {
			eventHeaders[parts[0]] = parts[1]
		}
	}

	// Extract event type
	eventType := eventHeaders["Event-Name"]
	if eventType == "" {
		LogError("Event type not found in body")
		return
	}

	// Extract event timestamp
	var eventTimestamp int64
	if timestamp := eventHeaders["Event-Date-Timestamp"]; timestamp != "" {
		if ts, err := strconv.ParseInt(timestamp, 10, 64); err == nil {
			eventTimestamp = ts * 1000 // Convert to nanoseconds
		}
	}

	// Determine stream based on event type
	var stream string
	if eslEventsToPublish[eventType] {
		// For BACKGROUND_JOB, check Event-Calling-Function
		if eventType == "BACKGROUND_JOB" {
			if callingFunction := eventHeaders["Event-Calling-Function"]; callingFunction != "api_exec" {
				return
			}
		}
		stream = config.Streams.Jobs.Name
	} else if eslEventsToPush[eventType] {
		stream = config.Streams.Events.Name
	} else {
		return
	}

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
		readTime:       time.Now(),
		eventTimestamp: eventTimestamp,
	}

	// Send message to channel
	select {
	case ch <- msg:
		LogDebug("Event sent to channel %s: %s", stream, string(eventJSON)[:100])
	case <-time.After(1 * time.Second):
		LogWarn("Timeout sending message to channel %s, buffer might be full", stream)
		// Try one more time without timeout
		select {
		case ch <- msg:
			LogDebug("Event sent to channel %s after retry: %s", stream, string(eventJSON)[:100])
		default:
			LogError("Failed to send message to channel %s, buffer is full", stream)
		}
	}
}
