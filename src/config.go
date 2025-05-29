package main

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"sync/atomic"
	"time"
)

var consumerCounter uint64

type Config struct {
	Redis struct {
		Local struct {
			Address      string
			Password     string
			DB           int
			PoolSize     int
			MinIdleConns int
			MaxRetries   int
		}
		Remote struct {
			Address      string
			Password     string
			DB           int
			PoolSize     int
			MinIdleConns int
			MaxRetries   int
		}
		Group    string
		Consumer string
	}
	Streams    []string
	Processing struct {
		// Reader configuration
		ReaderBatchSize  int64
		ReaderMaxLatency time.Duration
		ReaderBlockTime  time.Duration

		// Writer configuration
		WriterBatchSize       int
		WriterMaxLatency      time.Duration
		WriterPipelineTimeout time.Duration
		WriterWorkers         int

		// Buffer configuration
		BufferSize int

		// New total max latency configuration
		TotalMaxLatency time.Duration
	}
	Health struct {
		CheckInterval   time.Duration
		RecoveryTimeout time.Duration
		MaxRetries      int
		Port            int
	}
	Logging struct {
		Level  string
		Format string
		Output string
	}
}

func validateConfig(config *Config) error {
	// Redis validation
	if config.Redis.Local.Address == "" {
		return fmt.Errorf("local Redis address is required")
	}
	if config.Redis.Remote.Address == "" {
		return fmt.Errorf("remote Redis address is required")
	}
	if config.Redis.Group == "" {
		return fmt.Errorf("Redis group name is required")
	}
	if config.Redis.Consumer == "" {
		return fmt.Errorf("Redis consumer name is required")
	}

	// Streams validation
	if len(config.Streams) == 0 {
		return fmt.Errorf("at least one stream is required")
	}

	// Processing validation
	if config.Processing.BufferSize <= 0 {
		return fmt.Errorf("buffer size must be greater than 0")
	}
	if config.Processing.WriterWorkers <= 0 {
		return fmt.Errorf("writer workers must be greater than 0")
	}
	if config.Processing.ReaderMaxLatency <= 0 {
		return fmt.Errorf("reader max latency must be greater than 0")
	}
	if config.Processing.WriterMaxLatency <= 0 {
		return fmt.Errorf("writer max latency must be greater than 0")
	}
	if config.Processing.WriterPipelineTimeout <= 0 {
		return fmt.Errorf("writer pipeline timeout must be greater than 0")
	}

	return nil
}

func getConfig() Config {
	var config Config

	// Get hostname
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
	}

	// Generate incremental consumer ID
	consumerID := atomic.AddUint64(&consumerCounter, 1)

	// Redis Local
	config.Redis.Local.Address = getEnv("REDIS_LOCAL_ADDR", "localhost:6379")
	config.Redis.Local.Password = getEnv("REDIS_LOCAL_PASSWORD", "")
	config.Redis.Local.DB = getEnvAsInt("REDIS_LOCAL_DB", 2)
	config.Redis.Local.PoolSize = getEnvAsInt("REDIS_LOCAL_POOL_SIZE", 100)
	config.Redis.Local.MinIdleConns = getEnvAsInt("REDIS_LOCAL_MIN_IDLE_CONNS", 10)
	config.Redis.Local.MaxRetries = getEnvAsInt("REDIS_LOCAL_MAX_RETRIES", 3)

	// Redis Remote
	config.Redis.Remote.Address = getEnv("REDIS_REMOTE_ADDR", "redis.qa-uc-cloud1.gocontact.internal:6379")
	config.Redis.Remote.Password = getEnv("REDIS_REMOTE_PASSWORD", "")
	config.Redis.Remote.DB = getEnvAsInt("REDIS_REMOTE_DB", 2)
	config.Redis.Remote.PoolSize = getEnvAsInt("REDIS_REMOTE_POOL_SIZE", 100)
	config.Redis.Remote.MinIdleConns = getEnvAsInt("REDIS_REMOTE_MIN_IDLE_CONNS", 10)
	config.Redis.Remote.MaxRetries = getEnvAsInt("REDIS_REMOTE_MAX_RETRIES", 3)

	// Redis Consumer Group
	config.Redis.Group = getEnv("REDIS_GROUP", "sync_group")
	baseConsumer := getEnv("REDIS_CONSUMER", "sync_worker")
	config.Redis.Consumer = baseConsumer + "_" + hostname + "_" + strconv.FormatUint(consumerID, 10)

	// Streams
	config.Streams = []string{
		getEnv("STREAM_EVENTS", "freeswitch:telephony:events"),
		getEnv("STREAM_JOBS", "freeswitch:telephony:background-jobs"),
	}

	// Processing - Reader
	config.Processing.ReaderBatchSize = getEnvAsInt64("READER_BATCH_SIZE", 5000)
	config.Processing.ReaderMaxLatency = getEnvAsDuration("READER_MAX_LATENCY", 50*time.Millisecond)
	config.Processing.ReaderBlockTime = getEnvAsDuration("READER_BLOCK_TIME", 10*time.Millisecond)

	// Processing - Writer
	config.Processing.WriterBatchSize = getEnvAsInt("WRITER_BATCH_SIZE", 10)
	config.Processing.WriterMaxLatency = getEnvAsDuration("WRITER_MAX_LATENCY", 100*time.Millisecond)
	config.Processing.WriterPipelineTimeout = getEnvAsDuration("WRITER_PIPELINE_TIMEOUT", 25*time.Millisecond)
	config.Processing.WriterWorkers = getEnvAsInt("WRITER_WORKERS", 10)

	// Processing - Buffer
	config.Processing.BufferSize = getEnvAsInt("BUFFER_SIZE", 100000)

	// Processing - Total Max Latency
	config.Processing.TotalMaxLatency = getEnvAsDuration("TOTAL_MAX_LATENCY", 1000*time.Millisecond)

	// Health
	config.Health.CheckInterval = getEnvAsDuration("HEALTH_CHECK_INTERVAL", 5*time.Second)
	config.Health.RecoveryTimeout = getEnvAsDuration("HEALTH_RECOVERY_TIMEOUT", 30*time.Second)
	config.Health.MaxRetries = getEnvAsInt("HEALTH_MAX_RETRIES", 5)
	config.Health.Port = getEnvAsInt("HEALTH_PORT", 8080)

	// Logging
	config.Logging.Level = getEnv("LOG_LEVEL", "info")
	config.Logging.Format = getEnv("LOG_FORMAT", "json")
	config.Logging.Output = getEnv("LOG_OUTPUT", "stdout")

	// Validate configuration
	if err := validateConfig(&config); err != nil {
		log.Fatalf("Invalid configuration: %v", err)
	}

	return config
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvAsInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if intValue, err := strconv.Atoi(value); err == nil {
			return intValue
		}
	}
	return defaultValue
}

func getEnvAsInt64(key string, defaultValue int64) int64 {
	if value := os.Getenv(key); value != "" {
		if intValue, err := strconv.ParseInt(value, 10, 64); err == nil {
			return intValue
		}
	}
	return defaultValue
}

func getEnvAsDuration(key string, defaultValue time.Duration) time.Duration {
	if value := os.Getenv(key); value != "" {
		if duration, err := time.ParseDuration(value); err == nil {
			return duration
		}
	}
	return defaultValue
}
