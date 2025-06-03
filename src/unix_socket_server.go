package main

import (
	"context"
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
				LogError("Error accepting connection: %v", err)
				continue
			}

			// Handle connection in a goroutine
			go handleUnixConnection(conn, ch)
		}
	}
}

func handleUnixConnection(conn net.Conn, ch chan<- message) {
	defer conn.Close()

	buffer := make([]byte, 4096)
	for {
		n, err := conn.Read(buffer)
		if err != nil {
			LogError("Error reading from Unix socket: %v", err)
			return
		}

		if n > 0 {
			// Create message
			select {
			case ch <- message{
				stream:   "freeswitch_events",
				id:       time.Now().String(),
				values:   map[string]string{"event": string(buffer[:n])},
				readTime: time.Now(),
			}:
			default:
				LogWarn("Buffer full, message discarded from Unix socket")
			}
		}
	}
}
