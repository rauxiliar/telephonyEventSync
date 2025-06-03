package main

import (
	"bufio"
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"
)

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
			go handleUnixConnection(conn, ch)
		}
	}
}

func handleUnixConnection(conn net.Conn, ch chan<- message) {
	defer func() {
		LogInfo("Closing connection from %s", conn.RemoteAddr())
		conn.Close()
	}()

	LogInfo("New connection from %s", conn.RemoteAddr())

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

		// Create message
		msg := message{
			stream:   "freeswitch_events",
			id:       time.Now().String(),
			values:   map[string]string{"event": messageStr},
			readTime: time.Now(),
		}

		// Try to send message to channel with timeout
		select {
		case ch <- msg:
			LogInfo("Event sent to channel: %s", messageStr[:100]) // Log first 100 chars
		case <-time.After(1 * time.Second):
			LogWarn("Timeout sending message to channel, buffer might be full")
			// Try one more time without timeout
			select {
			case ch <- msg:
				LogInfo("Event sent to channel after retry: %s", messageStr[:100])
			default:
				LogError("Failed to send message to channel, buffer is full")
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
