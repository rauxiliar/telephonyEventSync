package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Default configuration values
const (
	// ESL defaults
	DefaultESLHost               = "127.0.0.1"
	DefaultESLPort               = 8021
	DefaultESLPassword           = "ClueCon"
	DefaultESLReconnectDelay     = 3 * time.Second
	DefaultESLMaxReconnectDelay  = 10 * time.Second
	DefaultESLHealthCheckTimeout = 3 * time.Second

	// Redis defaults
	DefaultRedisRemoteAddr       = "127.0.0.1:6379"
	DefaultRedisRemotePassword   = ""
	DefaultRedisRemoteDB         = 2
	DefaultRedisRemotePoolSize   = 100
	DefaultRedisRemoteMinIdle    = 20
	DefaultRedisRemoteMaxRetries = 3
	DefaultRedisWriteTimeout     = 2 * time.Second
	DefaultRedisDialTimeout      = 3 * time.Second
	DefaultRedisPoolTimeout      = 5 * time.Second

	// Health defaults
	DefaultHealthCheckInterval = 5 * time.Second
	DefaultHealthMaxRetries    = 100
	DefaultHealthPort          = 9876

	// Metrics defaults
	DefaultMetricsPrintInterval  = 5 * time.Second
	DefaultMetricsUpdateInterval = 1 * time.Second

	// Processing defaults
	DefaultBufferSize            = 50000
	DefaultReaderWorkers         = 3
	DefaultWriterWorkers         = 20
	DefaultWriterBatchSize       = 20
	DefaultWriterPipelineTimeout = 25 * time.Millisecond
	DefaultReaderMaxLatency      = 300 * time.Millisecond
	DefaultWriterMaxLatency      = 300 * time.Millisecond
	DefaultTotalMaxLatency       = 500 * time.Millisecond
	DefaultTrimInterval          = 10 * time.Second
	DefaultReaderBlockTime       = 10 * time.Millisecond
	DefaultEventsToPublish       = "BACKGROUND_JOB,CHANNEL_EXECUTE,CHANNEL_EXECUTE_COMPLETE,DTMF,DETECTED_SPEECH"
	DefaultEventsToPush          = "CHANNEL_ANSWER,CHANNEL_HANGUP,DTMF,CUSTOM"
	DefaultWriterMaxRetries      = 3
	DefaultWriterRetryPauseAfter = 100 * time.Millisecond
	DefaultWriterRetryQueueSize  = 10000
	DefaultWriterRetryTTL        = 5 * time.Second

	// Stream defaults
	DefaultStreamEventsName = "freeswitch:telephony:events"
	DefaultStreamJobsName   = "freeswitch:telephony:background-jobs"
	DefaultEventsMaxLen     = 10000
	DefaultJobsMaxLen       = 10000
	DefaultEventsExpireTime = 600 * time.Second // 10 minutes
	DefaultJobsExpireTime   = 60 * time.Second  // 1 minute
)

type StreamConfig struct {
	Name       string
	MaxLen     int64
	ExpireTime time.Duration
}

type Config struct {
	Redis struct {
		Remote struct {
			Address      string
			Password     string
			DB           int
			PoolSize     int
			MinIdleConns int
			MaxRetries   int
			// Timeout settings
			DialTimeout  time.Duration
			WriteTimeout time.Duration
			PoolTimeout  time.Duration
		}
	}
	ESL struct {
		Host     string
		Port     int
		Password string
		// Fast recovery settings
		ReconnectDelay      time.Duration
		MaxReconnectDelay   time.Duration
		HealthCheckInterval time.Duration
		HealthCheckTimeout  time.Duration
	}
	Streams struct {
		Events StreamConfig
		Jobs   StreamConfig
	}
	Processing struct {
		// Reader configuration
		ReaderMaxLatency time.Duration
		ReaderBlockTime  time.Duration
		ReaderWorkers    int

		// Writer configuration
		WriterBatchSize       int
		WriterMaxLatency      time.Duration
		WriterPipelineTimeout time.Duration
		WriterWorkers         int

		// Buffer configuration
		BufferSize int

		// New total max latency configuration
		TotalMaxLatency time.Duration

		// Trim interval
		TrimInterval time.Duration

		// ESL events configuration
		EventsToPublish []string
		EventsToPush    []string

		// Writer configuration
		WriterMaxRetries      int
		WriterRetryQueueSize  int
		WriterRetryPauseAfter time.Duration
		WriterRetryTTL        time.Duration
	}
	Health struct {
		CheckInterval time.Duration
		MaxRetries    int
		Port          int
	}
	Metrics struct {
		PrintInterval  time.Duration
		UpdateInterval time.Duration
	}
}

func validateConfig(config *Config) error {
	// Redis validation
	if config.Redis.Remote.Address == "" {
		return fmt.Errorf("remote Redis address is required")
	}

	// ESL validation
	if config.ESL.Host == "" {
		return fmt.Errorf("ESL host is required")
	}
	if config.ESL.Port <= 0 || config.ESL.Port > 65535 {
		return fmt.Errorf("ESL port must be between 1 and 65535, got %d", config.ESL.Port)
	}
	if config.ESL.Password == "" {
		return fmt.Errorf("ESL password is required")
	}

	// Streams validation
	if config.Streams.Events.Name == "" {
		return fmt.Errorf("stream events are required")
	}
	if config.Streams.Jobs.Name == "" {
		return fmt.Errorf("stream jobs are required")
	}

	// Processing validation
	if config.Processing.BufferSize <= 0 {
		return fmt.Errorf("buffer size must be greater than 0")
	}
	if config.Processing.ReaderWorkers <= 0 {
		return fmt.Errorf("reader workers must be greater than 0")
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

	// Health validation
	if config.Health.CheckInterval <= 0 {
		return fmt.Errorf("health check interval must be greater than 0")
	}
	if config.Health.MaxRetries < 0 {
		return fmt.Errorf("health max retries must be non-negative")
	}
	if config.Health.Port <= 0 || config.Health.Port > 65535 {
		return fmt.Errorf("health port must be between 1 and 65535, got %d", config.Health.Port)
	}

	return nil
}

func getConfig() Config {
	var config Config

	// ESL Configuration
	config.ESL.Host = getEnv("ESL_HOST", DefaultESLHost)
	config.ESL.Port = getEnvAsInt("ESL_PORT", DefaultESLPort)
	config.ESL.Password = getEnv("ESL_PASSWORD", DefaultESLPassword)
	config.ESL.ReconnectDelay = getEnvAsDuration("ESL_RECONNECT_DELAY", DefaultESLReconnectDelay)
	config.ESL.MaxReconnectDelay = getEnvAsDuration("ESL_MAX_RECONNECT_DELAY", DefaultESLMaxReconnectDelay)

	config.ESL.HealthCheckTimeout = getEnvAsDuration("ESL_HEALTH_CHECK_TIMEOUT", DefaultESLHealthCheckTimeout)

	// Redis Remote
	config.Redis.Remote.Address = getEnv("REDIS_REMOTE_ADDR", DefaultRedisRemoteAddr)
	config.Redis.Remote.Password = getEnv("REDIS_REMOTE_PASSWORD", DefaultRedisRemotePassword)
	config.Redis.Remote.DB = getEnvAsInt("REDIS_REMOTE_DB", DefaultRedisRemoteDB)
	config.Redis.Remote.PoolSize = getEnvAsInt("REDIS_REMOTE_POOL_SIZE", DefaultRedisRemotePoolSize)
	config.Redis.Remote.MinIdleConns = getEnvAsInt("REDIS_REMOTE_MIN_IDLE_CONNS", DefaultRedisRemoteMinIdle)
	config.Redis.Remote.MaxRetries = getEnvAsInt("REDIS_REMOTE_MAX_RETRIES", DefaultRedisRemoteMaxRetries)
	// Redis Remote Timeouts
	config.Redis.Remote.DialTimeout = getEnvAsDuration("REDIS_REMOTE_DIAL_TIMEOUT", DefaultRedisDialTimeout)
	config.Redis.Remote.WriteTimeout = getEnvAsDuration("REDIS_REMOTE_WRITE_TIMEOUT", DefaultRedisWriteTimeout)
	config.Redis.Remote.PoolTimeout = getEnvAsDuration("REDIS_REMOTE_POOL_TIMEOUT", DefaultRedisPoolTimeout)

	// Streams
	config.Streams.Events = StreamConfig{
		Name:       getEnv("STREAM_EVENTS", DefaultStreamEventsName),
		MaxLen:     getEnvAsInt64("EVENTS_MAX_LEN", DefaultEventsMaxLen),
		ExpireTime: getEnvAsDuration("EVENTS_EXPIRE_TIME", DefaultEventsExpireTime),
	}
	config.Streams.Jobs = StreamConfig{
		Name:       getEnv("STREAM_JOBS", DefaultStreamJobsName),
		MaxLen:     getEnvAsInt64("JOBS_MAX_LEN", DefaultJobsMaxLen),
		ExpireTime: getEnvAsDuration("JOBS_EXPIRE_TIME", DefaultJobsExpireTime),
	}

	// Processing - Reader
	config.Processing.ReaderMaxLatency = getEnvAsDuration("READER_MAX_LATENCY", DefaultReaderMaxLatency)
	config.Processing.ReaderBlockTime = getEnvAsDuration("READER_BLOCK_TIME", DefaultReaderBlockTime)
	config.Processing.ReaderWorkers = getEnvAsInt("READER_WORKERS", DefaultReaderWorkers)

	// Processing - Writer
	config.Processing.WriterBatchSize = getEnvAsInt("WRITER_BATCH_SIZE", DefaultWriterBatchSize)
	config.Processing.WriterMaxLatency = getEnvAsDuration("WRITER_MAX_LATENCY", DefaultWriterMaxLatency)
	config.Processing.WriterPipelineTimeout = getEnvAsDuration("WRITER_PIPELINE_TIMEOUT", DefaultWriterPipelineTimeout)
	config.Processing.WriterWorkers = getEnvAsInt("WRITER_WORKERS", DefaultWriterWorkers)

	// Processing - Buffer
	config.Processing.BufferSize = getEnvAsInt("BUFFER_SIZE", DefaultBufferSize)

	// Processing - Total Max Latency
	config.Processing.TotalMaxLatency = getEnvAsDuration("TOTAL_MAX_LATENCY", DefaultTotalMaxLatency)

	// Processing - Trim Interval
	config.Processing.TrimInterval = getEnvAsDuration("TRIM_INTERVAL", DefaultTrimInterval)

	// Processing - ESL Events
	config.Processing.EventsToPublish = getEnvAsStringSlice("EVENTS_TO_PUBLISH", DefaultEventsToPublish)
	config.Processing.EventsToPush = getEnvAsStringSlice("EVENTS_TO_PUSH", DefaultEventsToPush)

	// Processing - Writer configuration
	config.Processing.WriterMaxRetries = getEnvAsInt("WRITER_MAX_RETRIES", DefaultWriterMaxRetries)
	config.Processing.WriterRetryPauseAfter = getEnvAsDuration("WRITER_RETRY_PAUSE_AFTER", DefaultWriterRetryPauseAfter)
	config.Processing.WriterRetryQueueSize = getEnvAsInt("WRITER_RETRY_QUEUE_SIZE", DefaultWriterRetryQueueSize)
	config.Processing.WriterRetryTTL = getEnvAsDuration("WRITER_RETRY_TTL", DefaultWriterRetryTTL)

	// Health
	config.Health.CheckInterval = getEnvAsDuration("HEALTH_CHECK_INTERVAL", DefaultHealthCheckInterval)
	config.Health.MaxRetries = getEnvAsInt("HEALTH_MAX_RETRIES", DefaultHealthMaxRetries)
	config.Health.Port = getEnvAsInt("HEALTH_PORT", DefaultHealthPort)

	// Metrics
	config.Metrics.PrintInterval = getEnvAsDuration("METRICS_PRINT_INTERVAL", DefaultMetricsPrintInterval)
	config.Metrics.UpdateInterval = getEnvAsDuration("METRICS_UPDATE_INTERVAL", DefaultMetricsUpdateInterval)

	// Validate configuration
	if err := validateConfig(&config); err != nil {
		LogError("Invalid configuration: %v", err)
		os.Exit(1)
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

func getEnvAsStringSlice(key string, defaultValue string) []string {
	if value := os.Getenv(key); value != "" {
		return strings.Split(value, ",")
	}
	return strings.Split(defaultValue, ",")
}

// GetESLHealthCheckTimeout returns the ESL health check timeout with fallback
func (c *Config) GetESLHealthCheckTimeout() time.Duration {
	if c.ESL.HealthCheckTimeout == 0 {
		return DefaultESLHealthCheckTimeout
	}
	return c.ESL.HealthCheckTimeout
}

// GetESLReconnectDelay returns the ESL reconnect delay with fallback
func (c *Config) GetESLReconnectDelay() time.Duration {
	if c.ESL.ReconnectDelay == 0 {
		return DefaultESLReconnectDelay
	}
	return c.ESL.ReconnectDelay
}

// GetESLMaxReconnectDelay returns the ESL max reconnect delay with fallback
func (c *Config) GetESLMaxReconnectDelay() time.Duration {
	if c.ESL.MaxReconnectDelay == 0 {
		return DefaultESLMaxReconnectDelay
	}
	return c.ESL.MaxReconnectDelay
}

// GetRedisRemoteWriteTimeout returns the Redis remote write timeout with fallback
func (c *Config) GetRedisRemoteWriteTimeout() time.Duration {
	if c.Redis.Remote.WriteTimeout == 0 {
		return DefaultRedisWriteTimeout
	}
	return c.Redis.Remote.WriteTimeout
}

// GetRedisRemoteDialTimeout returns the Redis remote dial timeout with fallback
func (c *Config) GetRedisRemoteDialTimeout() time.Duration {
	if c.Redis.Remote.DialTimeout == 0 {
		return DefaultRedisDialTimeout
	}
	return c.Redis.Remote.DialTimeout
}

// GetHealthCheckInterval returns the health check interval with fallback
func (c *Config) GetHealthCheckInterval() time.Duration {
	if c.Health.CheckInterval == 0 {
		return DefaultHealthCheckInterval
	}
	return c.Health.CheckInterval
}

// GetHealthMaxRetries returns the health max retries with fallback
func (c *Config) GetHealthMaxRetries() int {
	if c.Health.MaxRetries == 0 {
		return DefaultHealthMaxRetries
	}
	return c.Health.MaxRetries
}

// GetHealthPort returns the health port with fallback
func (c *Config) GetHealthPort() int {
	if c.Health.Port == 0 {
		return DefaultHealthPort
	}
	return c.Health.Port
}

// GetMetricsPrintInterval returns the metrics print interval with fallback
func (c *Config) GetMetricsPrintInterval() time.Duration {
	if c.Metrics.PrintInterval == 0 {
		return DefaultMetricsPrintInterval
	}
	return c.Metrics.PrintInterval
}

// GetMetricsUpdateInterval returns the metrics update interval with fallback
func (c *Config) GetMetricsUpdateInterval() time.Duration {
	if c.Metrics.UpdateInterval == 0 {
		return DefaultMetricsUpdateInterval
	}
	return c.Metrics.UpdateInterval
}

func (c *Config) GetHealthCheckTimeout() time.Duration {
	if c.Health.CheckInterval == 0 {
		return DefaultHealthCheckInterval
	}
	return c.Health.CheckInterval
}
