package main

import (
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/op/go-logging"
)

const (
	LogLevelError = iota
	LogLevelWarn
	LogLevelInfo
	LogLevelDebug
)

type AsyncLogger struct {
	logChan    chan string
	bufferSize int
	logLevel   int
	stopChan   chan struct{}
	wg         sync.WaitGroup
}

var (
	globalLogger *AsyncLogger
	once         sync.Once
)

// initLogger initializes the global logger instance
func initLogger() *AsyncLogger {
	once.Do(func() {
		globalLogger = createAsyncLogger()
		globalLogger.startProcessing()
	})

	return globalLogger
}

// createAsyncLogger creates a new AsyncLogger with configuration from environment
func createAsyncLogger() *AsyncLogger {
	bufferSize := getLogBufferSize()
	logLevel := getLogLevel()

	return &AsyncLogger{
		logChan:    make(chan string, bufferSize),
		bufferSize: bufferSize,
		logLevel:   logLevel,
		stopChan:   make(chan struct{}),
	}
}

// getLogBufferSize reads buffer size from environment variable
func getLogBufferSize() int {
	bufferSize := 1000
	if size := os.Getenv("LOG_BUFFER_SIZE"); size != "" {
		if s, err := fmt.Sscanf(size, "%d", &bufferSize); err != nil || s != 1 {
			bufferSize = 1000
		}
	}
	return bufferSize
}

// getLogLevel reads log level from environment variable
func getLogLevel() int {
	logLevel := LogLevelInfo
	if level := os.Getenv("LOG_LEVEL"); level != "" {
		logLevel = parseLogLevel(level)
	}
	return logLevel
}

// parseLogLevel converts string log level to integer constant
func parseLogLevel(level string) int {
	switch level {
	case "error":
		return LogLevelError
	case "warn":
		return LogLevelWarn
	case "info":
		return LogLevelInfo
	case "debug":
		return LogLevelDebug
	default:
		return LogLevelInfo
	}
}

// startProcessing starts the background log processing goroutine
func (l *AsyncLogger) startProcessing() {
	l.wg.Add(1)
	go l.process()
}

// process handles log message processing in background
func (l *AsyncLogger) process() {
	defer l.wg.Done()

	for {
		// BLOCKING: Waits for log entries or stop signal
		select {
		case entry := <-l.logChan:
			log.Print(entry)
		case <-l.stopChan:
			l.processRemainingLogs()
			return
		}
	}
}

// processRemainingLogs processes any remaining logs before shutdown
func (l *AsyncLogger) processRemainingLogs() {
	for {
		// BLOCKING: Drains remaining log entries before shutdown
		select {
		case entry := <-l.logChan:
			log.Print(entry)
		default:
			return
		}
	}
}

// Log logs a message at the specified level
func (l *AsyncLogger) Log(level int, format string, args ...any) {
	if level > l.logLevel {
		return
	}

	var levelStr string
	switch level {
	case LogLevelError:
		levelStr = "ERROR"
	case LogLevelWarn:
		levelStr = "WARN"
	case LogLevelInfo:
		levelStr = "INFO"
	case LogLevelDebug:
		levelStr = "DEBUG"
	default:
		levelStr = "UNKNOWN"
	}

	entry := fmt.Sprintf("[%s] %s", levelStr, fmt.Sprintf(format, args...))
	select {
	case l.logChan <- entry:
	default:
		// Channel full, discard log
	}
}

// Shutdown gracefully shuts down the logger
func (l *AsyncLogger) Shutdown() {
	close(l.stopChan)
	// BLOCKING: Waits for all logger goroutines to finish processing logs
	l.wg.Wait()
}

// Convenience functions for global logging
func LogError(format string, args ...any) {
	globalLogger.Log(LogLevelError, format, args...)
}

func LogWarn(format string, args ...any) {
	globalLogger.Log(LogLevelWarn, format, args...)
}

func LogInfo(format string, args ...any) {
	globalLogger.Log(LogLevelInfo, format, args...)
}

func LogDebug(format string, args ...any) {
	globalLogger.Log(LogLevelDebug, format, args...)
}

// LogWithContext logs a message with additional context fields
func LogWithContext(level logging.Level, message string, fields map[string]any) {
	logEntry := formatLogWithContext(message, fields)
	logLevel := convertLoggingLevel(level)
	globalLogger.Log(logLevel, "%s", logEntry)
}

// formatLogWithContext formats log message with context fields
func formatLogWithContext(message string, fields map[string]any) string {
	if len(fields) == 0 {
		return message
	}

	fieldPairs := formatContextFields(fields)
	return fmt.Sprintf("%s | Context: %s", message, strings.Join(fieldPairs, " "))
}

// formatContextFields formats context fields for logging
func formatContextFields(fields map[string]any) []string {
	var fieldPairs []string
	for k, v := range fields {
		fieldPairs = append(fieldPairs, fmt.Sprintf("%s:%v", k, v))
	}
	return fieldPairs
}

// convertLoggingLevel converts go-logging level to internal level
func convertLoggingLevel(level logging.Level) int {
	switch level {
	case logging.DEBUG:
		return LogLevelDebug
	case logging.INFO:
		return LogLevelInfo
	case logging.WARNING:
		return LogLevelWarn
	case logging.ERROR:
		return LogLevelError
	default:
		return LogLevelInfo
	}
}

// LogLatency logs latency information with context only when threshold is exceeded
func LogLatency(stage string, latency time.Duration, threshold time.Duration, fields map[string]any) {
	if latency <= threshold {
		return
	}

	baseMessage := fmt.Sprintf("LATENCY WARNING | %s: %s > %s", stage, latency.String(), threshold.String())

	message := baseMessage
	if uuidVal, ok := fields["uuid"]; ok {
		message = fmt.Sprintf("%s | UUID: %s", message, fmt.Sprintf("%v", uuidVal))
		delete(fields, "uuid")
	}
	if eventTypeVal, ok := fields["event_type"]; ok {
		message = fmt.Sprintf("%s | Event: %s", message, fmt.Sprintf("%v", eventTypeVal))
		delete(fields, "event_type")
	}
	for k, v := range fields {
		message = fmt.Sprintf("%s | %s: %v", message, k, v)
	}

	LogWarn("%s", message)
}
