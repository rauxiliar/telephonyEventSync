package main

import (
	"sync"
	"time"
)

// message represents a telephony event message
type message struct {
	uuid           string
	stream         string
	values         map[string]string
	readTime       time.Time // Timestamp when the message was read and processed
	eventTimestamp int64     // Original event timestamp
	eventType      string    // Event type for logging and metrics
	retries        int       // Number of retry attempts made
}

// Metrics tracks application metrics
type Metrics struct {
	sync.Mutex
	messagesProcessed  int64
	errors             int64
	lastSyncTime       time.Time
	readerChannelSize  int   // Size of the reader channel
	writerChannelSize  int   // Size of the writer channel
	eslConnections     int64 // Number of ESL connections established
	eslReconnections   int64 // Number of ESL reconnections
	redisConnections   int64 // Number of Redis connections established
	redisReconnections int64 // Number of Redis reconnections
}

// latencyCheck represents a latency measurement check
type latencyCheck struct {
	uuid      string
	timestamp int64
	eventType string
}
