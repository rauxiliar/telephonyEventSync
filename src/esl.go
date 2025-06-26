package main

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/0x19/goesl"
)

var (
	globalESLClient *ESLClient
	eslClientMutex  sync.RWMutex
	eslRecoveryChan = make(chan struct{}, 1) // Recovery channel, buffer 1 to avoid blocking
)

// ESLClient represents an ESL connection to FreeSWITCH
type ESLClient struct {
	client *goesl.Client
	config Config
}

// NewESLClient creates a new ESL client instance
// It creates a new ESL client
// It returns an error if the client creation fails
func NewESLClient(config Config) (*ESLClient, error) {
	client, err := goesl.NewClient(config.ESL.Host, uint(config.ESL.Port), config.ESL.Password, 1000)
	if err != nil {
		return nil, &ESLConnectionError{
			Host: config.ESL.Host,
			Port: config.ESL.Port,
			Err:  err,
		}
	}

	return &ESLClient{
		client: client,
		config: config,
	}, nil
}

// SetupAndConnect establishes the connection with FreeSWITCH and configures event subscriptions
// It starts the client handler
// It builds the event list
// It subscribes to the events
// It verifies the subscription
// It returns an error if the setup fails
func (e *ESLClient) SetupAndConnect() error {
	LogInfo("Starting ESL client handler")
	e.startClientHandler()

	events := e.buildEventList()
	if err := e.subscribeToEvents(events); err != nil {
		return err
	}

	if err := e.verifySubscription(); err != nil {
		return err
	}

	LogInfo("Successfully subscribed to events")
	return nil
}

// startClientHandler starts the ESL client handler goroutine
// It starts the client handler
func (e *ESLClient) startClientHandler() {
	go e.client.Handle()
}

// buildEventList builds the list of events to subscribe to
// It adds events to publish
// It adds events to push
// It returns the list of events
func (e *ESLClient) buildEventList() []string {
	var events []string
	// Add events to publish
	events = append(events, e.config.Processing.EventsToPublish...)
	// Add events to push
	events = append(events, e.config.Processing.EventsToPush...)
	return events
}

// subscribeToEvents subscribes to the specified events
// It sends the event command to the ESL client
// It returns an error if the subscription fails
func (e *ESLClient) subscribeToEvents(events []string) error {
	eventCmd := fmt.Sprintf("events json %s", strings.Join(events, " "))
	LogInfo("Subscribing to events: %s", eventCmd)

	if err := e.client.Send(eventCmd); err != nil {
		return &ESLConnectionError{
			Host: e.config.ESL.Host,
			Port: e.config.ESL.Port,
			Err:  fmt.Errorf("failed to subscribe to events: %v", err),
		}
	}

	return nil
}

// verifySubscription verifies that the event subscription was successful
// It reads the subscription reply
// It checks if the reply is successful
// It returns an error if the subscription fails
func (e *ESLClient) verifySubscription() error {
	reply, err := e.client.ReadMessage()
	if err != nil {
		return &ESLConnectionError{
			Host: e.config.ESL.Host,
			Port: e.config.ESL.Port,
			Err:  fmt.Errorf("failed to read subscription reply: %v", err),
		}
	}

	replyText := reply.GetHeader("Reply-Text")
	if !strings.HasPrefix(replyText, "+OK") {
		return &ESLConnectionError{
			Host: e.config.ESL.Host,
			Port: e.config.ESL.Port,
			Err:  fmt.Errorf("unexpected subscription reply: %s", replyText),
		}
	}

	return nil
}

// Close closes the ESL connection
// It closes the ESL client
func (e *ESLClient) Close() {
	if e.client != nil {
		e.client.Close()
	}
}

// ReadEvents starts reading events from FreeSWITCH
// It starts the event reading loop
// It returns an error if the reading fails
func (e *ESLClient) ReadEvents(ctx context.Context, eventsChan chan<- *goesl.Message, recoveryChan <-chan struct{}) {
	readDone := make(chan struct{})

	go e.startEventReading(ctx, eventsChan, readDone)
	<-readDone
}

// startEventReading starts the event reading loop
// It reads events from the ESL connection
// It processes each event
// It returns an error if the reading fails
func (e *ESLClient) startEventReading(ctx context.Context, eventsChan chan<- *goesl.Message, readDone chan struct{}) {
	defer close(readDone)

	for {
		// BLOCKING: Reads events from ESL connection, blocks until event arrives or context is cancelled
		select {
		case <-ctx.Done():
			return
		default:
			if !e.readAndProcessEvent(eventsChan) {
				return
			}
		}
	}
}

// readAndProcessEvent reads and processes a single event
// It reads an event from the ESL connection
// It processes the event
// It returns true if the event is processed successfully
func (e *ESLClient) readAndProcessEvent(eventsChan chan<- *goesl.Message) bool {
	evt, err := e.client.ReadMessage()

	if err != nil {
		return e.handleReadError(err)
	}

	if evt.GetHeader("Content-Type") == "text/disconnect-notice" {
		LogWarn("Received disconnect notice from FreeSWITCH: %s", string(evt.Body))
		return false
	}

	eventsChan <- evt
	return true
}

// handleReadError handles errors during event reading
// It checks if the error is a closed connection
// It logs the error
// It returns true if the error is handled successfully
func (e *ESLClient) handleReadError(err error) bool {
	if !isClosedError(err) {
		LogError("Error reading ESL event: %v (connection to %s:%d)", err, e.config.ESL.Host, e.config.ESL.Port)
	}
	return false
}

// GetESLClient returns the global ESL client for health monitoring
// It returns the global ESL client
func GetESLClient() *ESLClient {
	eslClientMutex.RLock()
	defer eslClientMutex.RUnlock()
	return globalESLClient
}

// setGlobalESLClient sets the global ESL client
// It sets the global ESL client
func setGlobalESLClient(client *ESLClient) {
	eslClientMutex.Lock()
	defer eslClientMutex.Unlock()
	globalESLClient = client
}

// closeESLConnections closes ESL connection and updates metrics
// It closes the ESL connection
// It updates the ESL connection metrics
func closeESLConnections() {
	if eslClient := GetESLClient(); eslClient != nil {
		eslClient.Close()
		GetMetricsManager().SetESLConnections(0)
		LogInfo("ESL connection closed")
	}
}

// isClosedError checks if the error indicates a closed connection
// It checks if the error is a closed connection
// It returns true if the error is a closed connection
func isClosedError(err error) bool {
	if err == nil {
		return false
	}
	return err.Error() == "EOF" || err.Error() == "use of closed network connection"
}
