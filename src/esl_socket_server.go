package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fiorix/go-eventsocket/eventsocket"
)

// Event types that should be published to background-jobs stream
var eslEventsToPublish = map[string]bool{
	"BACKGROUND_JOB":           true, // Will be filtered by Event-Calling-Function
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

type ringBuffer struct {
	buffer     []*eventsocket.Event
	size       int
	head       int
	tail       int
	count      int
	mu         sync.Mutex
	notEmpty   *sync.Cond
	notFull    *sync.Cond
	putCount   atomic.Int64
	getCount   atomic.Int64
	putLatency atomic.Int64
	getLatency atomic.Int64
}

func newRingBuffer(size int) *ringBuffer {
	rb := &ringBuffer{
		buffer: make([]*eventsocket.Event, size),
		size:   size,
	}
	rb.notEmpty = sync.NewCond(&rb.mu)
	rb.notFull = sync.NewCond(&rb.mu)
	return rb
}

func (rb *ringBuffer) put(event *eventsocket.Event) {
	start := time.Now()
	rb.mu.Lock()
	defer rb.mu.Unlock()

	for rb.count == rb.size {
		rb.notFull.Wait()
	}

	rb.buffer[rb.tail] = event
	rb.tail = (rb.tail + 1) % rb.size
	rb.count++
	rb.putCount.Add(1)
	rb.putLatency.Add(time.Since(start).Nanoseconds())
	rb.notEmpty.Signal()
}

func (rb *ringBuffer) get() *eventsocket.Event {
	start := time.Now()
	rb.mu.Lock()
	defer rb.mu.Unlock()

	for rb.count == 0 {
		rb.notEmpty.Wait()
	}

	event := rb.buffer[rb.head]
	rb.head = (rb.head + 1) % rb.size
	rb.count--
	rb.getCount.Add(1)
	rb.getLatency.Add(time.Since(start).Nanoseconds())
	rb.notFull.Signal()
	return event
}

func (rb *ringBuffer) metrics() string {
	return fmt.Sprintf("Buffer: %d/%d, Put: %d (%.2fms), Get: %d (%.2fms)",
		rb.count, rb.size,
		rb.putCount.Load(),
		float64(rb.putLatency.Load())/float64(rb.putCount.Load())/1e6,
		rb.getCount.Load(),
		float64(rb.getLatency.Load())/float64(rb.getCount.Load())/1e6)
}

func eslSocketServer(ctx context.Context, ch chan<- message, wg *sync.WaitGroup, config Config) {
	defer wg.Done()

	// Create ESL client
	client, err := eventsocket.Dial(fmt.Sprintf("%s:%d", config.ESL.Host, config.ESL.Port), config.ESL.Password)
	if err != nil {
		LogError("Failed to create ESL client: %v", err)
		return
	}
	defer client.Close()

	// Build event list from our maps
	var events []string
	for event := range eslEventsToPublish {
		events = append(events, event)
	}
	for event := range eslEventsToPush {
		events = append(events, event)
	}

	// Subscribe to specific events
	eventCmd := fmt.Sprintf("events json %s", strings.Join(events, " "))
	if _, err := client.Send(eventCmd); err != nil {
		LogError("Failed to subscribe to events: %v", err)
		return
	}

	LogInfo("ESL server started and connected to FreeSWITCH at %s:%d", config.ESL.Host, config.ESL.Port)

	// Process events
	for {
		select {
		case <-ctx.Done():
			LogInfo("Stopping ESL server")
			return
		default:
			evt, err := client.ReadEvent()
			if err != nil {
				if !isClosedError(err) {
					LogError("Error reading ESL event: %v (connection to %s:%d)", err, config.ESL.Host, config.ESL.Port)
				}
				continue
			}

			// Process event in a new goroutine
			go func(evt *eventsocket.Event) {
				if evt == nil {
					LogError("Received nil event")
					return
				}

				// Ignore command replies
				if evt.Get("Content-Type") == "command/reply" {
					return
				}

				// Get event type directly from headers
				eventType := evt.Get("Event-Name")
				if eventType == "" {
					LogError("Event type not found in headers. Headers: %+v", evt)
					return
				}

				// Extract event timestamp
				var eventTimestamp int64
				if timestamp := evt.Get("Event-Date-Timestamp"); timestamp != "" {
					if ts, err := strconv.ParseInt(timestamp, 10, 64); err == nil {
						eventTimestamp = ts * 1000 // Convert to nanoseconds
					}
				}

				// Check reader latency
				readTime := time.Now()
				eventTime := time.Unix(0, eventTimestamp)
				readerLatency := readTime.Sub(eventTime)
				if readerLatency > config.Processing.ReaderMaxLatency {
					LogWarn("High reader latency since event trigger detected for ESL socket message: %v", readerLatency)
				}

				// Determine stream based on event type
				var stream string
				if eslEventsToPublish[eventType] {
					// For BACKGROUND_JOB, check Event-Calling-Function
					if eventType == "BACKGROUND_JOB" {
						if callingFunction := evt.Get("Event-Calling-Function"); callingFunction != "api_exec" {
							return
						}
					}
					stream = config.Streams.Jobs.Name
				} else if eslEventsToPush[eventType] {
					stream = config.Streams.Events.Name
				} else {
					return
				}

				// Convert headers to JSON directly
				eventJSON, err := json.Marshal(evt.Header)
				if err != nil {
					LogError("Error serializing event: %v", err)
					return
				}

				// Create message
				msg := message{
					stream:         stream,
					values:         map[string]string{"event": string(eventJSON)},
					readTime:       readTime,
					eventTimestamp: eventTimestamp,
				}

				// Send message to channel with shorter timeout
				select {
				case ch <- msg:
					// Message sent successfully
				case <-time.After(50 * time.Millisecond):
					// Try one more time without timeout
					select {
					case ch <- msg:
						// Message sent after retry
					default:
						LogError("Failed to send message to channel %s, buffer is full", stream)
					}
				}
			}(evt)
		}
	}
}
