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

// connectionState holds the state for ESL connection management
type connectionState struct {
	reconnectDelay     time.Duration
	maxReconnectDelay  time.Duration
	connectionAttempts int
}

// StartESLConnection starts and maintains the ESL connection to FreeSWITCH
func StartESLConnection(ctx context.Context, ch chan<- message, wg *sync.WaitGroup, config Config) {
	defer wg.Done()

	// initializeESLLogging sets up ESL logging configuration
	logging.SetLevel(logging.ERROR, "goesl")

	connectionState := connectionState{
		reconnectDelay:     config.GetESLReconnectDelay(),
		maxReconnectDelay:  config.GetESLMaxReconnectDelay(),
		connectionAttempts: 0,
	}
	eslEventsChan := make(chan *goesl.Message, config.Processing.BufferSize)
	globalReaderChan = eslEventsChan // Set global reader channel for metrics
	workerWg := StartWorkers(ctx, eslEventsChan, ch, config, config.Processing.ReaderProcessingWorkers)

	for {
		select {
		case <-ctx.Done():
			LogInfo("Stopping ESL server")
			close(eslEventsChan)
			workerWg.Wait()
			return
		default:
			handleESLConnectionAttempt(ctx, eslEventsChan, ch, config, &connectionState)
		}
	}
}

// handleESLConnectionAttempt handles a single connection attempt
// It attempts to connect to FreeSWITCH
// It handles connection errors and delays
// It handles successful connections
// It handles connection loss
func handleESLConnectionAttempt(ctx context.Context, eslEventsChan chan *goesl.Message, ch chan<- message, config Config, state *connectionState) {
	state.connectionAttempts++
	LogInfo("Attempting to connect to FreeSWITCH (attempt %d) at %s:%d", state.connectionAttempts, config.ESL.Host, config.ESL.Port)

	client, err := NewESLClient(config)
	if err != nil {
		LogError("Failed to create ESL client: %v", err)
		handleReconnectDelay(state)
		return
	}

	if err := establishESLConnection(client); err != nil {
		LogError("Failed to connect: %v", err)
		client.Close()
		handleReconnectDelay(state)
		return
	}

	handleESLSuccessfulConnection(client, config, state)
	// BLOCKING: ReadEvents blocks until connection is lost or context is cancelled
	// This function will only return when the ESL connection is broken
	client.ReadEvents(ctx, eslEventsChan, eslRecoveryChan)
	// This line is only reached when ReadEvents returns (connection lost)
	handleESLConnectionLoss(client, state)
}

// handleReconnectDelay handles the reconnection delay logic
// It implements exponential backoff before reconnection attempt
func handleReconnectDelay(state *connectionState) {
	// BLOCKING: Sleep to implement exponential backoff before reconnection attempt
	time.Sleep(state.reconnectDelay)
	if state.reconnectDelay < state.maxReconnectDelay {
		state.reconnectDelay *= 2
	}
}

// establishESLConnection establishes the ESL connection
// It sets up and connects to FreeSWITCH
// It returns an error if the connection fails
func establishESLConnection(client *ESLClient) error {
	if err := client.SetupAndConnect(); err != nil {
		LogError("Failed to connect: %v", err)
		client.Close()
		return err
	}
	return nil
}

// handleESLSuccessfulConnection handles successful connection setup
func handleESLSuccessfulConnection(client *ESLClient, config Config, state *connectionState) {
	setGlobalESLClient(client)
	updateESLConnectionMetrics(state.connectionAttempts)
	resetESLConnectionState(state, config)
	LogInfo("Successfully connected to FreeSWITCH at %s:%d", config.ESL.Host, config.ESL.Port)
}

// updateESLConnectionMetrics updates connection-related metrics
// It updates the ESL connection metrics
// It sets the ESL connection count to 1
// It increments the ESL reconnection count if the connection attempt is greater than 1
func updateESLConnectionMetrics(connectionAttempts int) {
	metricsManager := GetMetricsManager()
	metricsManager.SetESLConnections(1) // 1 = connected
	if connectionAttempts > 1 {
		metricsManager.IncrementESLReconnections()
	}
}

// resetESLConnectionState resets the connection state after successful connection
// It resets the reconnection delay
// It resets the connection attempts
func resetESLConnectionState(state *connectionState, config Config) {
	state.reconnectDelay = config.GetESLReconnectDelay()
	state.connectionAttempts = 0
}

// handleESLConnectionLoss handles the loss of ESL connection
// It sets the global ESL client to nil
// It sets the ESL connection count to 0
// It closes the ESL client
// It logs the connection loss and attempts to reconnect
func handleESLConnectionLoss(client *ESLClient, state *connectionState) {
	setGlobalESLClient(nil)                  // Clear global client
	GetMetricsManager().SetESLConnections(0) // 0 = disconnected
	client.Close()
	LogInfo("ESL connection lost, attempting to reconnect in %v...", state.reconnectDelay)
	// BLOCKING: Sleep before attempting reconnection to avoid overwhelming the server
	time.Sleep(state.reconnectDelay)
}

// StartWorkers initializes the worker pool to process events
// It creates a wait group for the workers
// It adds the worker count to the wait group
// It starts the worker goroutines
// It returns the wait group
func StartWorkers(ctx context.Context, eventsChan <-chan *goesl.Message, outputChan chan<- message, config Config, workerCount int) *sync.WaitGroup {
	var workerWg sync.WaitGroup
	workerWg.Add(workerCount)

	for range workerCount {
		go startWorker(eventsChan, outputChan, config, &workerWg)
	}

	return &workerWg
}

// startWorker starts a single worker goroutine
// It processes events from the input channel
// It processes each event in a loop
// It recovers from panics and decrements the wait group
func startWorker(eventsChan <-chan *goesl.Message, outputChan chan<- message, config Config, workerWg *sync.WaitGroup) {
	defer PanicRecoveryFunc("ESL worker")()
	defer workerWg.Done()

	for evt := range eventsChan {
		processESLEvent(evt, outputChan, config)
	}
}

// processESLEvent processes an ESL event
// It determines the appropriate stream for the event
// It extracts the event UUID and timestamp
// It creates a message from the event data
// It sends the message to the output channel
func processESLEvent(evt *goesl.Message, ch chan<- message, config Config) {
	readTime := time.Now()
	eventType := evt.GetHeader("Event-Name")

	stream := determineEventStream(eventType, evt, config)
	if stream == "" {
		return
	}

	uuid := extractEventUUID(evt)
	eventTimestamp := processEventTimestamp(evt, readTime, uuid, eventType, config)
	eventMap := createEventMap(evt)
	eventJSON := marshalEventToJSON(eventMap)

	if eventJSON == "" {
		return
	}

	msg := createMessage(uuid, stream, eventJSON, readTime, eventTimestamp, eventType)
	sendMessageToChannel(msg, ch, stream, config)
}

// determineEventStream determines which stream the event should go to
// It returns the stream name for the event
// It checks if the event should be published or pushed
// It also handles special cases for BACKGROUND_JOB events
func determineEventStream(eventType string, evt *goesl.Message, config Config) string {
	eventsStreamName := config.Streams.Events.Name
	jobsStreamName := config.Streams.Jobs.Name

	if isEventToPublish(eventType, config) {
		if eventType == "BACKGROUND_JOB" {
			if evt.GetHeader("Event-Calling-Function") != "api_exec" {
				return ""
			}
		}
		return jobsStreamName
	} else if isEventToPush(eventType, config) {
		return eventsStreamName
	}

	return ""
}

// processEventTimestamp processes the event timestamp and calculates latency
// It returns the event timestamp in nanoseconds
// It also calculates and logs the reader latency
func processEventTimestamp(evt *goesl.Message, readTime time.Time, uuid, eventType string, config Config) int64 {
	if timestamp := evt.GetHeader("Event-Date-Timestamp"); timestamp != "" {
		if ts, err := strconv.ParseInt(timestamp, 10, 64); err == nil {
			eventTimestamp := ts * 1000 // Convert microseconds to nanoseconds
			calculateAndLogLatency(readTime, eventTimestamp, uuid, eventType, config)
			return eventTimestamp
		}
	}
	return 0
}

// calculateAndLogLatency calculates and logs reader latency
// It calculates the latency between the event time and the read time
// It logs the latency to the global logger
func calculateAndLogLatency(readTime time.Time, eventTimestamp int64, uuid, eventType string, config Config) {
	eventTime := time.Unix(0, eventTimestamp)
	readerLatency := readTime.Sub(eventTime)

	promReaderLatency.Observe(float64(readerLatency.Milliseconds()))
	LogLatency("reader", readerLatency, config.Processing.ReaderMaxLatency, map[string]any{
		"uuid":       uuid,
		"event_type": eventType,
		"event_time": eventTime.Format(time.RFC3339),
		"read_time":  readTime.Format(time.RFC3339),
	})
}

// createEventMap creates a map from the ESL event
// It creates a map from the event headers
// It adds the event body if it exists
func createEventMap(evt *goesl.Message) map[string]string {
	headerCount := len(evt.Headers)
	eventMap := make(map[string]string, headerCount+1) // +1 for potential body
	maps.Copy(eventMap, evt.Headers)

	// Add body if exists
	if len(evt.Body) > 0 {
		eventMap["_body"] = string(evt.Body)
	}

	return eventMap
}

// marshalEventToJSON marshals the event map to JSON
// It marshals the event map to JSON
// It logs an error if the event map cannot be marshaled
func marshalEventToJSON(eventMap map[string]string) string {
	eventJSON, err := json.Marshal(eventMap)
	if err != nil {
		LogError("Failed to marshal event to JSON: %v", err)
		return ""
	}
	return string(eventJSON)
}

// createMessage creates a message from the event data
// It creates a message from the event data
// It sets the message values
func createMessage(uuid, stream, eventJSON string, readTime time.Time, eventTimestamp int64, eventType string) message {
	values := map[string]string{
		"event": eventJSON,
	}

	return message{
		uuid:           uuid,
		stream:         stream,
		values:         values,
		readTime:       readTime,
		eventTimestamp: eventTimestamp,
		eventType:      eventType,
	}
}

// sendMessageToChannel sends the message to the output channel
// Implements a two-stage retry mechanism to handle channel back pressure:
// 1. First attempt: Try to send immediately with a timeout
// 2. Second attempt: If timeout occurs, try one more time with non-blocking send
// This prevents the ESL reader from blocking indefinitely when the channel is full,
// ensuring that event processing continues even under high load conditions.
// If both attempts fail, the message is logged as lost to prevent memory buildup.
// It sends the message to the output channel
func sendMessageToChannel(msg message, ch chan<- message, stream string, config Config) {
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
// It extracts the event UUID from the event
func extractEventUUID(evt *goesl.Message) string {
	// Try Job-UUID first (for BACKGROUND_JOB events)
	if uuid := evt.GetHeader("Job-UUID"); uuid != "" {
		return uuid
	}

	// Use GetCallUUID() which checks Caller-Unique-ID
	return evt.GetCallUUID()
}

// Helper functions to check if event should be published/pushed
// It checks if the event should be published
func isEventToPublish(eventType string, config Config) bool {
	return slices.Contains(config.Processing.EventsToPublish, eventType)
}

// isEventToPush checks if the event should be pushed
// It checks if the event type is in the push events list
func isEventToPush(eventType string, config Config) bool {
	return slices.Contains(config.Processing.EventsToPush, eventType)
}
