package main

import (
	"fmt"
	"log"
	"os"
	"sync"
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

func initLogger() *AsyncLogger {
	once.Do(func() {
		bufferSize := 1000
		if size := os.Getenv("LOG_BUFFER_SIZE"); size != "" {
			if s, err := fmt.Sscanf(size, "%d", &bufferSize); err != nil || s != 1 {
				bufferSize = 1000
			}
		}

		logLevel := LogLevelInfo
		if level := os.Getenv("LOG_LEVEL"); level != "" {
			switch level {
			case "error":
				logLevel = LogLevelError
			case "warn":
				logLevel = LogLevelWarn
			case "info":
				logLevel = LogLevelInfo
			case "debug":
				logLevel = LogLevelDebug
			}
		}

		globalLogger = &AsyncLogger{
			logChan:    make(chan string, bufferSize),
			bufferSize: bufferSize,
			logLevel:   logLevel,
			stopChan:   make(chan struct{}),
		}

		globalLogger.wg.Add(1)
		go globalLogger.process()
	})

	return globalLogger
}

func (l *AsyncLogger) process() {
	defer l.wg.Done()

	for {
		select {
		case entry := <-l.logChan:
			log.Print(entry)
		case <-l.stopChan:
			// Process remaining logs
			for {
				select {
				case entry := <-l.logChan:
					log.Print(entry)
				default:
					return
				}
			}
		}
	}
}

func (l *AsyncLogger) Log(level int, format string, args ...interface{}) {
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
	}

	entry := fmt.Sprintf("[%s] %s", levelStr, fmt.Sprintf(format, args...))

	select {
	case l.logChan <- entry:
	default:
		// Channel full, discard log
	}
}

func (l *AsyncLogger) Shutdown() {
	close(l.stopChan)
	l.wg.Wait()
}

// Convenience functions
func LogError(format string, args ...interface{}) {
	globalLogger.Log(LogLevelError, format, args...)
}

func LogWarn(format string, args ...interface{}) {
	globalLogger.Log(LogLevelWarn, format, args...)
}

func LogInfo(format string, args ...interface{}) {
	globalLogger.Log(LogLevelInfo, format, args...)
}

func LogDebug(format string, args ...interface{}) {
	globalLogger.Log(LogLevelDebug, format, args...)
}
