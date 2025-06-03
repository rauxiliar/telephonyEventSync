package main

import (
	"context"
	"net"
	"os"
	"sync"
	"time"
)

func unixSocketServer(ctx context.Context, ch chan<- message, wg *sync.WaitGroup, id int, config Config) {
	defer wg.Done()

	// Remove socket file if it exists
	os.Remove("/tmp/telephony.sock")

	// Create Unix socket listener
	listener, err := net.Listen("unix", "/tmp/telephony.sock")
	if err != nil {
		LogError("Error creating Unix socket: %v", err)
		return
	}
	defer listener.Close()

	// Set socket permissions
	if err := os.Chmod("/tmp/telephony.sock", 0666); err != nil {
		LogError("Error setting socket permissions: %v", err)
		return
	}

	LogInfo("Unix socket server started at /tmp/telephony.sock")

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
