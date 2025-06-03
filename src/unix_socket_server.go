package main

import (
	"bufio"
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

// Event types that should be published to background-jobs stream
var eventsToPublish = map[string]bool{
	"BACKGROUND_JOB":           true, // Will be filtered by Event-Calling-Function
	"CHANNEL_EXECUTE":          true,
	"CHANNEL_EXECUTE_COMPLETE": true,
	"DTMF":                     true,
	"DETECTED_SPEECH":          true,
}

// Event types that should be pushed to events stream
var eventsToPush = map[string]bool{
	"CHANNEL_ANSWER": true,
	"CHANNEL_HANGUP": true,
	"DTMF":           true,
	"CUSTOM":         true,
}

func unixSocketServer(ctx context.Context, ch chan<- message, wg *sync.WaitGroup, id int, config Config) {
	defer wg.Done()

	// Get socket path from config or use default
	socketPath := config.Unix.SocketPath
	if socketPath == "" {
		socketPath = "/var/run/telephony/telephony.sock"
	}

	// Ensure directory exists
	socketDir := filepath.Dir(socketPath)
	if err := os.MkdirAll(socketDir, 0755); err != nil {
		LogError("Error creating socket directory: %v", err)
		return
	}

	// Remove socket file if it exists
	os.Remove(socketPath)

	// Create Unix socket listener
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		LogError("Error creating Unix socket: %v", err)
		return
	}
	defer listener.Close()

	// Set socket permissions
	if err := os.Chmod(socketPath, 0666); err != nil {
		LogError("Error setting socket permissions: %v", err)
		return
	}

	LogInfo("Unix socket server started at %s", socketPath)

	// Accept connections
	for {
		select {
		case <-ctx.Done():
			LogInfo("Stopping Unix socket server")
			return
		default:
			conn, err := listener.Accept()
			if err != nil {
				if !isClosedError(err) {
					LogError("Error accepting connection: %v", err)
				}
				continue
			}

			// Handle connection in a goroutine
			go handleUnixConnection(conn, ch, config)
		}
	}
}

func handleUnixConnection(conn net.Conn, ch chan<- message, config Config) {
	defer func() {
		LogDebug("Closing connection from %s", conn.RemoteAddr())
		conn.Close()
	}()

	LogDebug("New connection from %s", conn.RemoteAddr())

	reader := bufio.NewReader(conn)
	for {
		// Set read deadline
		conn.SetReadDeadline(time.Now().Add(5 * time.Second))

		// Read message
		messageStr, err := reader.ReadString('\n')
		if err != nil {
			if !isClosedError(err) {
				LogError("Error reading from Unix socket: %v", err)
			}
			return
		}

		// Parse message
		var event map[string]interface{}
		if err := json.Unmarshal([]byte(messageStr), &event); err != nil {
			LogError("Error parsing message: %v", err)
			continue
		}

		// Get event type
		eventType, ok := event["Event-Name"].(string)
		if !ok {
			LogError("Event type not found in message")
			continue
		}

		// Extract event timestamp
		var eventTimestamp int64
		if timestamp, ok := event["Event-Date-Timestamp"].(string); ok {
			if ts, err := strconv.ParseInt(timestamp, 10, 64); err == nil {
				eventTimestamp = ts * 1000 // Convert to nanoseconds
			}
		}

		// Determine stream based on event type
		var stream string
		if eventsToPublish[eventType] {
			// For BACKGROUND_JOB, check Event-Calling-Function
			if eventType == "BACKGROUND_JOB" {
				if callingFunction, ok := event["Event-Calling-Function"].(string); !ok || callingFunction != "api_exec" {
					LogDebug("Skipping BACKGROUND_JOB event with calling function: %v", callingFunction)
					continue
				}
			}
			stream = config.Streams.Jobs.Name
		} else if eventsToPush[eventType] {
			stream = config.Streams.Events.Name
		} else {
			LogDebug("Skipping event of type: %s", eventType)
			continue
		}

		// Create message
		msg := message{
			stream:         stream,
			values:         map[string]string{"event": messageStr},
			readTime:       time.Now(),
			eventTimestamp: eventTimestamp,
		}

		// Try to send message to channel with timeout
		select {
		case ch <- msg:
			LogDebug("Event sent to channel %s: %s", stream, messageStr[:100]) // Log first 100 chars
		case <-time.After(1 * time.Second):
			LogWarn("Timeout sending message to channel %s, buffer might be full", stream)
			// Try one more time without timeout
			select {
			case ch <- msg:
				LogDebug("Event sent to channel %s after retry: %s", stream, messageStr[:100])
			default:
				LogError("Failed to send message to channel %s, buffer is full", stream)
			}
		}
	}
}

func isClosedError(err error) bool {
	if err == nil {
		return false
	}
	return err.Error() == "EOF" || err.Error() == "use of closed network connection"
}
