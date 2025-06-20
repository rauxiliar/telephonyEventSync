package main

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
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

// ESLClient represents an ESL connection to FreeSWITCH
type ESLClient struct {
	client *goesl.Client
	config Config
}

// NewESLClient creates a new ESL client instance
func NewESLClient(config Config) (*ESLClient, error) {
	client, err := goesl.NewClient(config.ESL.Host, uint(config.ESL.Port), config.ESL.Password, 100)
	if err != nil {
		return nil, fmt.Errorf("failed to create ESL client: %v", err)
	}

	return &ESLClient{
		client: client,
		config: config,
	}, nil
}

// SetupAndConnect establishes the connection with FreeSWITCH and configures event subscriptions
func (e *ESLClient) SetupAndConnect() error {
	LogInfo("Starting ESL client handler")
	go e.client.Handle()

	var events []string
	for event := range eslEventsToPublish {
		events = append(events, event)
	}
	for event := range eslEventsToPush {
		events = append(events, event)
	}

	eventCmd := fmt.Sprintf("events json %s", strings.Join(events, " "))
	LogInfo("Subscribing to events: %s", eventCmd)
	if err := e.client.Send(eventCmd); err != nil {
		return fmt.Errorf("failed to subscribe to events: %v", err)
	}

	// Verify if the subscription was successful
	reply, err := e.client.ReadMessage()
	if err != nil {
		return fmt.Errorf("failed to read subscription reply: %v", err)
	}

	replyText := reply.GetHeader("Reply-Text")
	if !strings.HasPrefix(replyText, "+OK") {
		return fmt.Errorf("unexpected subscription reply: %s", replyText)
	}

	LogInfo("Successfully subscribed to events")
	return nil
}

// Close closes the ESL connection
func (e *ESLClient) Close() {
	if e.client != nil {
		e.client.Close()
	}
}

// ReadEvents starts reading events from FreeSWITCH
func (e *ESLClient) ReadEvents(ctx context.Context, eventsChan chan<- *goesl.Message) {
	readDone := make(chan struct{})
	go func() {
		defer close(readDone)
		for {
			select {
			case <-ctx.Done():
				return
			default:
				evt, err := e.client.ReadMessage()
				if err != nil {
					if !isClosedError(err) {
						LogError("Error reading ESL event: %v (connection to %s:%d)", err, e.config.ESL.Host, e.config.ESL.Port)
					}
					return
				}

				// Check for disconnect notice
				if evt.GetHeader("Content-Type") == "text/disconnect-notice" {
					LogWarn("Received disconnect notice from FreeSWITCH: %s", string(evt.Body))
					return
				}

				select {
				case eventsChan <- evt:
				default:
					LogWarn("ESL events channel full, event dropped")
				}
			}
		}
	}()

	<-readDone
}

// StartWorkers initializes the worker pool to process events
func StartWorkers(ctx context.Context, eventsChan <-chan *goesl.Message, outputChan chan<- message, config Config, workerCount int) *sync.WaitGroup {
	var workerWg sync.WaitGroup
	workerWg.Add(workerCount)

	for range workerCount {
		go func() {
			defer workerWg.Done()
			for evt := range eventsChan {
				processESLEvent(evt, outputChan, config)
			}
		}()
	}

	return &workerWg
}

// StartMetricsUpdater starts the periodic metrics update
func StartMetricsUpdater(ctx context.Context, eventsChan chan *goesl.Message) {
	metricsUpdateTicker := time.NewTicker(1 * time.Second)
	defer metricsUpdateTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-metricsUpdateTicker.C:
			metrics.Lock()
			metrics.readerChannelSize = len(eventsChan)
			metrics.Unlock()
		}
	}
}

// StartESLConnection starts and maintains the ESL connection to FreeSWITCH
func StartESLConnection(ctx context.Context, ch chan<- message, wg *sync.WaitGroup, config Config) {
	defer wg.Done()

	logging.SetLevel(logging.ERROR, "goesl")

	reconnectDelay := 5 * time.Second
	maxReconnectDelay := 30 * time.Second

	// Create events channel
	eslEventsChan := make(chan *goesl.Message, config.Processing.BufferSize)

	// Initialize worker pool with the events channel
	workerWg := StartWorkers(ctx, eslEventsChan, ch, config, config.Processing.ReaderWorkers)

	// Start metrics updater with the events channel
	go StartMetricsUpdater(ctx, eslEventsChan)

	for {
		select {
		case <-ctx.Done():
			LogInfo("Stopping ESL server")
			close(eslEventsChan)
			workerWg.Wait()
			return
		default:
			client, err := NewESLClient(config)
			if err != nil {
				LogError("Failed to create ESL client: %v", err)
				time.Sleep(reconnectDelay)
				if reconnectDelay < maxReconnectDelay {
					reconnectDelay *= 2
				}
				continue
			}

			// Reset reconnect delay on successful connection
			reconnectDelay = 5 * time.Second

			if err := client.SetupAndConnect(); err != nil {
				LogError("Failed to connect: %v", err)
				client.Close()
				time.Sleep(reconnectDelay)
				continue
			}

			LogInfo("Successfully connected to FreeSWITCH at %s:%d", config.ESL.Host, config.ESL.Port)

			// Start reading events and forward them to the events channel
			client.ReadEvents(ctx, eslEventsChan)

			// If we get here, the connection was lost
			client.Close()
			LogInfo("ESL connection lost, attempting to reconnect in %v...", reconnectDelay)
			time.Sleep(reconnectDelay)
		}
	}
}

func processESLEvent(evt *goesl.Message, ch chan<- message, config Config) {
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

	eventType := evt.GetHeader("Event-Name")
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

	// Create event map with all headers
	eventMap := make(map[string]string)
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
		stream:         stream,
		values:         values,
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
