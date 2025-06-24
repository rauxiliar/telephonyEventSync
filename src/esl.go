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
func NewESLClient(config Config) (*ESLClient, error) {
	client, err := goesl.NewClient(config.ESL.Host, uint(config.ESL.Port), config.ESL.Password, 100)
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
func (e *ESLClient) SetupAndConnect() error {
	LogInfo("Starting ESL client handler")
	go e.client.Handle()

	var events []string
	// Add events to publish
	events = append(events, e.config.Processing.EventsToPublish...)
	// Add events to push
	events = append(events, e.config.Processing.EventsToPush...)

	eventCmd := fmt.Sprintf("events json %s", strings.Join(events, " "))
	LogInfo("Subscribing to events: %s", eventCmd)
	if err := e.client.Send(eventCmd); err != nil {
		return &ESLConnectionError{
			Host: e.config.ESL.Host,
			Port: e.config.ESL.Port,
			Err:  fmt.Errorf("failed to subscribe to events: %v", err),
		}
	}

	// Verify if the subscription was successful
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
func (e *ESLClient) ReadEvents(ctx context.Context, eventsChan chan<- *goesl.Message, recoveryChan <-chan struct{}) {
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

				eventsChan <- evt
			}
		}
	}()

	<-readDone
}

// GetESLClient returns the global ESL client for health monitoring
func GetESLClient() *ESLClient {
	eslClientMutex.RLock()
	defer eslClientMutex.RUnlock()
	return globalESLClient
}

// setGlobalESLClient sets the global ESL client
func setGlobalESLClient(client *ESLClient) {
	eslClientMutex.Lock()
	defer eslClientMutex.Unlock()
	globalESLClient = client
}

// closeESLConnections closes ESL connection and updates metrics
func closeESLConnections() {
	if eslClient := GetESLClient(); eslClient != nil {
		eslClient.Close()
		GetMetricsManager().SetESLConnections(0)
		LogInfo("ESL connection closed")
	}
}

func isClosedError(err error) bool {
	if err == nil {
		return false
	}
	return err.Error() == "EOF" || err.Error() == "use of closed network connection"
}
