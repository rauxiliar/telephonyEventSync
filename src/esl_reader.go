package main

import (
	"context"
	"encoding/json"
	"strconv"
	"sync"
	"time"

	"maps"
	"slices"

	"github.com/0x19/goesl"
	"github.com/op/go-logging"
)

// StartESLConnection starts and maintains the ESL connection to FreeSWITCH
func StartESLConnection(ctx context.Context, ch chan<- message, wg *sync.WaitGroup, config Config) {
	defer wg.Done()

	logging.SetLevel(logging.ERROR, "goesl")

	// Fast recovery settings from config
	reconnectDelay := config.GetESLReconnectDelay()
	maxReconnectDelay := config.GetESLMaxReconnectDelay()
	connectionAttempts := 0

	// Create events channel
	eslEventsChan := make(chan *goesl.Message, config.Processing.BufferSize)
	globalReaderChan = eslEventsChan // Set global reader channel for metrics

	// Initialize worker pool with the events channel
	workerWg := StartWorkers(ctx, eslEventsChan, ch, config, config.Processing.ReaderProcessingWorkers)

	for {
		select {
		case <-ctx.Done():
			LogInfo("Stopping ESL server")
			close(eslEventsChan)
			workerWg.Wait()
			return
		default:
			connectionAttempts++
			LogInfo("Attempting to connect to FreeSWITCH (attempt %d) at %s:%d", connectionAttempts, config.ESL.Host, config.ESL.Port)

			client, err := NewESLClient(config)
			if err != nil {
				LogError("Failed to create ESL client: %v", err)
				time.Sleep(reconnectDelay)
				if reconnectDelay < maxReconnectDelay {
					reconnectDelay *= 2
				}
				continue
			}

			if err := client.SetupAndConnect(); err != nil {
				LogError("Failed to connect: %v", err)
				client.Close()
				time.Sleep(reconnectDelay)
				if reconnectDelay < maxReconnectDelay {
					reconnectDelay *= 2
				}
				continue
			}

			// Set global ESL client for health monitoring
			setGlobalESLClient(client)

			// Update metrics - set current connection state
			metricsManager := GetMetricsManager()
			metricsManager.SetESLConnections(1) // 1 = connected
			if connectionAttempts > 1 {
				metricsManager.IncrementESLReconnections()
			}

			// Reset reconnect delay on successful connection
			reconnectDelay = config.GetESLReconnectDelay()
			connectionAttempts = 0

			LogInfo("Successfully connected to FreeSWITCH at %s:%d", config.ESL.Host, config.ESL.Port)

			// Start reading events and forward them to the events channel
			client.ReadEvents(ctx, eslEventsChan, eslRecoveryChan)

			// If we get here, the connection was lost
			setGlobalESLClient(nil)             // Clear global client
			metricsManager.SetESLConnections(0) // 0 = disconnected
			client.Close()
			LogInfo("ESL connection lost, attempting to reconnect in %v...", reconnectDelay)
			time.Sleep(reconnectDelay)
		}
	}
}

// StartWorkers initializes the worker pool to process events
func StartWorkers(ctx context.Context, eventsChan <-chan *goesl.Message, outputChan chan<- message, config Config, workerCount int) *sync.WaitGroup {
	var workerWg sync.WaitGroup
	workerWg.Add(workerCount)

	for range workerCount {
		go func() {
			defer PanicRecoveryFunc("ESL worker")()
			defer workerWg.Done()
			for evt := range eventsChan {
				processESLEvent(evt, outputChan, config)
			}
		}()
	}

	return &workerWg
}

func processESLEvent(evt *goesl.Message, ch chan<- message, config Config) {
	readTime := time.Now()

	// Generate unique message ID
	eventType := evt.GetHeader("Event-Name")

	eventsStreamName := config.Streams.Events.Name
	jobsStreamName := config.Streams.Jobs.Name

	var stream string
	if isEventToPublish(eventType, config) {
		if eventType == "BACKGROUND_JOB" {
			if evt.GetHeader("Event-Calling-Function") != "api_exec" {
				return
			}
		}
		stream = jobsStreamName
	} else if isEventToPush(eventType, config) {
		stream = eventsStreamName
	} else {
		return
	}

	// Extract UUID based on event type
	uuid := extractEventUUID(evt)

	// Only calculate latency if we have a valid timestamp
	var eventTimestamp int64
	if timestamp := evt.GetHeader("Event-Date-Timestamp"); timestamp != "" {
		if ts, err := strconv.ParseInt(timestamp, 10, 64); err == nil {
			eventTimestamp = ts * 1000 // Convert microseconds to nanoseconds
			eventTime := time.Unix(0, eventTimestamp)
			readerLatency := readTime.Sub(eventTime)

			ObserveReaderLatency(float64(readerLatency.Milliseconds()))
			LogLatency("reader", readerLatency, config.Processing.ReaderMaxLatency, map[string]any{
				"uuid":       uuid,
				"event_type": eventType,
				"event_time": eventTime.Format(time.RFC3339),
				"read_time":  readTime.Format(time.RFC3339),
			})
		}
	}

	// Pre-allocate map with estimated capacity to avoid reallocations
	headerCount := len(evt.Headers)
	eventMap := make(map[string]string, headerCount+1) // +1 for potential body
	maps.Copy(eventMap, evt.Headers)

	// Add body if exists
	if len(evt.Body) > 0 {
		eventMap["_body"] = string(evt.Body)
	}

	// Convert event map to JSON string
	eventJSON, err := json.Marshal(eventMap)
	if err != nil {
		LogError("Failed to marshal event to JSON: %v", err)
		return
	}

	// Create the final event structure
	values := map[string]string{
		"event": string(eventJSON),
	}

	msg := message{
		uuid:           uuid,
		stream:         stream,
		values:         values,
		readTime:       readTime,
		eventTimestamp: eventTimestamp,
		eventType:      eventType,
	}

	select {
	case ch <- msg:
	case <-time.After(config.Processing.ReaderBlockTime):
		select {
		case ch <- msg:
		default:
			LogError("Failed to send message to channel %s, buffer is full", stream)
		}
	}
}

// extractEventUUID extracts the appropriate UUID based on event type
// Optimized with early returns and minimal string comparisons
func extractEventUUID(evt *goesl.Message) string {
	// Try Job-UUID first (for BACKGROUND_JOB events)
	if uuid := evt.GetHeader("Job-UUID"); uuid != "" {
		return uuid
	}

	// Use GetCallUUID() which checks Caller-Unique-ID
	return evt.GetCallUUID()
}

// Helper functions to check if event should be published/pushed
func isEventToPublish(eventType string, config Config) bool {
	return slices.Contains(config.Processing.EventsToPublish, eventType)
}

func isEventToPush(eventType string, config Config) bool {
	return slices.Contains(config.Processing.EventsToPush, eventType)
}
