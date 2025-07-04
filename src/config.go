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
	DefaultRedisRemotePoolSize   = 50
	DefaultRedisRemoteMinIdle    = 10
	DefaultRedisRemoteMaxRetries = 3
	DefaultRedisWriteTimeout     = 3 * time.Second
	DefaultRedisDialTimeout      = 3 * time.Second
	DefaultRedisPoolTimeout      = 5 * time.Second

	// Health defaults
	DefaultHealthCheckInterval = 5 * time.Second
	DefaultHealthMaxRetries    = 100
	DefaultHealthPort          = 9876

	// Metrics defaults
	DefaultMetricsPrintInterval = 5 * time.Second

	// Processing defaults
	DefaultBufferSize              = 50000
	DefaultReaderProcessingWorkers = 3
	DefaultWriterWorkers           = 10
	DefaultWriterBatchSize         = 20
	DefaultWriterPipelineTimeout   = 25 * time.Millisecond
	DefaultReaderMaxLatency        = 300 * time.Millisecond
	DefaultWriterMaxLatency        = 300 * time.Millisecond
	DefaultTotalMaxLatency         = 500 * time.Millisecond
	DefaultTrimInterval            = 10 * time.Second
	DefaultReaderBlockTime         = 10 * time.Millisecond
	DefaultEventsToPublish         = "BACKGROUND_JOB,CHANNEL_EXECUTE,CHANNEL_EXECUTE_COMPLETE,DTMF,DETECTED_SPEECH"
	DefaultEventsToPush            = "CHANNEL_ANSWER,CHANNEL_HANGUP,DTMF,CUSTOM"
	DefaultWriterMaxRetries        = 3
	DefaultWriterRetryPauseAfter   = 100 * time.Millisecond
	DefaultWriterRetryQueueSize    = 10000
	DefaultWriterRetryTTL          = 5 * time.Second

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
		ReaderMaxLatency        time.Duration
		ReaderBlockTime         time.Duration
		ReaderProcessingWorkers int

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
		PrintInterval time.Duration
	}
}

func validateConfig(config *Config) error {
	if err := validateRedisConfig(config); err != nil {
		return err
	}
	if err := validateESLConfig(config); err != nil {
		return err
	}
	if err := validateStreamsConfig(config); err != nil {
		return err
	}
	if err := validateProcessingConfig(config); err != nil {
		return err
	}
	if err := validateHealthConfig(config); err != nil {
		return err
	}
	return nil
}

// validateRedisConfig validates Redis configuration
func validateRedisConfig(config *Config) error {
	if config.Redis.Remote.Address == "" {
		return &ConfigurationError{
			Field: "REDIS_REMOTE_ADDR",
			Value: "",
			Err:   fmt.Errorf("remote Redis address is required"),
		}
	}
	return nil
}

// validateESLConfig validates ESL configuration
func validateESLConfig(config *Config) error {
	if config.ESL.Host == "" {
		return &ConfigurationError{
			Field: "ESL_HOST",
			Value: "",
			Err:   fmt.Errorf("ESL host is required"),
		}
	}
	if config.ESL.Port <= 0 || config.ESL.Port > 65535 {
		return &ConfigurationError{
			Field: "ESL_PORT",
			Value: fmt.Sprintf("%d", config.ESL.Port),
			Err:   fmt.Errorf("ESL port must be between 1 and 65535"),
		}
	}
	if config.ESL.Password == "" {
		return &ConfigurationError{
			Field: "ESL_PASSWORD",
			Value: "",
			Err:   fmt.Errorf("ESL password is required"),
		}
	}
	return nil
}

// validateStreamsConfig validates streams configuration
func validateStreamsConfig(config *Config) error {
	if config.Streams.Events.Name == "" {
		return &ConfigurationError{
			Field: "STREAM_EVENTS",
			Value: "",
			Err:   fmt.Errorf("stream events are required"),
		}
	}
	if config.Streams.Jobs.Name == "" {
		return &ConfigurationError{
			Field: "STREAM_JOBS",
			Value: "",
			Err:   fmt.Errorf("stream jobs are required"),
		}
	}
	return nil
}

// validateProcessingConfig validates processing configuration
func validateProcessingConfig(config *Config) error {
	if config.Processing.BufferSize <= 0 {
		return &ConfigurationError{
			Field: "BUFFER_SIZE",
			Value: fmt.Sprintf("%d", config.Processing.BufferSize),
			Err:   fmt.Errorf("buffer size must be greater than 0"),
		}
	}
	if config.Processing.ReaderProcessingWorkers <= 0 {
		return &ConfigurationError{
			Field: "READER_PROCESSING_WORKERS",
			Value: fmt.Sprintf("%d", config.Processing.ReaderProcessingWorkers),
			Err:   fmt.Errorf("reader processing workers must be greater than 0"),
		}
	}
	if config.Processing.WriterWorkers <= 0 {
		return &ConfigurationError{
			Field: "WRITER_WORKERS",
			Value: fmt.Sprintf("%d", config.Processing.WriterWorkers),
			Err:   fmt.Errorf("writer workers must be greater than 0"),
		}
	}
	if config.Processing.ReaderMaxLatency <= 0 {
		return &ConfigurationError{
			Field: "READER_MAX_LATENCY",
			Value: config.Processing.ReaderMaxLatency.String(),
			Err:   fmt.Errorf("reader max latency must be greater than 0"),
		}
	}
	if config.Processing.WriterMaxLatency <= 0 {
		return &ConfigurationError{
			Field: "WRITER_MAX_LATENCY",
			Value: config.Processing.WriterMaxLatency.String(),
			Err:   fmt.Errorf("writer max latency must be greater than 0"),
		}
	}
	if config.Processing.WriterPipelineTimeout <= 0 {
		return &ConfigurationError{
			Field: "WRITER_PIPELINE_TIMEOUT",
			Value: config.Processing.WriterPipelineTimeout.String(),
			Err:   fmt.Errorf("writer pipeline timeout must be greater than 0"),
		}
	}
	return nil
}

// validateHealthConfig validates health configuration
func validateHealthConfig(config *Config) error {
	if config.Health.CheckInterval <= 0 {
		return &ConfigurationError{
			Field: "HEALTH_CHECK_INTERVAL",
			Value: config.Health.CheckInterval.String(),
			Err:   fmt.Errorf("health check interval must be greater than 0"),
		}
	}
	if config.Health.MaxRetries < 0 {
		return &ConfigurationError{
			Field: "HEALTH_MAX_RETRIES",
			Value: fmt.Sprintf("%d", config.Health.MaxRetries),
			Err:   fmt.Errorf("health max retries must be non-negative"),
		}
	}
	if config.Health.Port <= 0 || config.Health.Port > 65535 {
		return &ConfigurationError{
			Field: "HEALTH_PORT",
			Value: fmt.Sprintf("%d", config.Health.Port),
			Err:   fmt.Errorf("health port must be between 1 and 65535"),
		}
	}
	return nil
}

func getConfig() Config {
	var config Config

	loadESLConfig(&config)
	loadRedisConfig(&config)
	loadStreamsConfig(&config)
	loadProcessingConfig(&config)
	loadHealthConfig(&config)
	loadMetricsConfig(&config)

	// Validate configuration
	if err := validateConfig(&config); err != nil {
		LogError("Invalid configuration: %v", err)
		os.Exit(1)
	}

	return config
}

// loadESLConfig loads ESL configuration from environment variables
func loadESLConfig(config *Config) {
	config.ESL.Host = getEnv("ESL_HOST", DefaultESLHost)
	config.ESL.Port = getEnvAsInt("ESL_PORT", DefaultESLPort)
	config.ESL.Password = getEnv("ESL_PASSWORD", DefaultESLPassword)
	config.ESL.ReconnectDelay = getEnvAsDuration("ESL_RECONNECT_DELAY", DefaultESLReconnectDelay)
	config.ESL.MaxReconnectDelay = getEnvAsDuration("ESL_MAX_RECONNECT_DELAY", DefaultESLMaxReconnectDelay)
	config.ESL.HealthCheckTimeout = getEnvAsDuration("ESL_HEALTH_CHECK_TIMEOUT", DefaultESLHealthCheckTimeout)
}

// loadRedisConfig loads Redis configuration from environment variables
func loadRedisConfig(config *Config) {
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
}

// loadStreamsConfig loads streams configuration from environment variables
func loadStreamsConfig(config *Config) {
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
}

// loadProcessingConfig loads processing configuration from environment variables
func loadProcessingConfig(config *Config) {
	loadProcessingReaderConfig(config)
	loadProcessingWriterConfig(config)
	loadProcessingBufferConfig(config)
	loadProcessingEventsConfig(config)
	loadProcessingRetryConfig(config)
}

// loadProcessingReaderConfig loads reader processing configuration
func loadProcessingReaderConfig(config *Config) {
	config.Processing.ReaderMaxLatency = getEnvAsDuration("READER_MAX_LATENCY", DefaultReaderMaxLatency)
	config.Processing.ReaderBlockTime = getEnvAsDuration("READER_BLOCK_TIME", DefaultReaderBlockTime)
	config.Processing.ReaderProcessingWorkers = getEnvAsInt("READER_PROCESSING_WORKERS", DefaultReaderProcessingWorkers)
}

// loadProcessingWriterConfig loads writer processing configuration
func loadProcessingWriterConfig(config *Config) {
	config.Processing.WriterBatchSize = getEnvAsInt("WRITER_BATCH_SIZE", DefaultWriterBatchSize)
	config.Processing.WriterMaxLatency = getEnvAsDuration("WRITER_MAX_LATENCY", DefaultWriterMaxLatency)
	config.Processing.WriterPipelineTimeout = getEnvAsDuration("WRITER_PIPELINE_TIMEOUT", DefaultWriterPipelineTimeout)
	config.Processing.WriterWorkers = getEnvAsInt("WRITER_WORKERS", DefaultWriterWorkers)
}

// loadProcessingBufferConfig loads buffer processing configuration
func loadProcessingBufferConfig(config *Config) {
	config.Processing.BufferSize = getEnvAsInt("BUFFER_SIZE", DefaultBufferSize)
	config.Processing.TotalMaxLatency = getEnvAsDuration("TOTAL_MAX_LATENCY", DefaultTotalMaxLatency)
	config.Processing.TrimInterval = getEnvAsDuration("TRIM_INTERVAL", DefaultTrimInterval)
}

// loadProcessingEventsConfig loads events processing configuration
func loadProcessingEventsConfig(config *Config) {
	config.Processing.EventsToPublish = getEnvAsStringSlice("EVENTS_TO_PUBLISH", DefaultEventsToPublish)
	config.Processing.EventsToPush = getEnvAsStringSlice("EVENTS_TO_PUSH", DefaultEventsToPush)
}

// loadProcessingRetryConfig loads retry processing configuration
func loadProcessingRetryConfig(config *Config) {
	config.Processing.WriterMaxRetries = getEnvAsInt("WRITER_MAX_RETRIES", DefaultWriterMaxRetries)
	config.Processing.WriterRetryPauseAfter = getEnvAsDuration("WRITER_RETRY_PAUSE_AFTER", DefaultWriterRetryPauseAfter)
	config.Processing.WriterRetryQueueSize = getEnvAsInt("WRITER_RETRY_QUEUE_SIZE", DefaultWriterRetryQueueSize)
	config.Processing.WriterRetryTTL = getEnvAsDuration("WRITER_RETRY_TTL", DefaultWriterRetryTTL)
}

// loadHealthConfig loads health configuration from environment variables
func loadHealthConfig(config *Config) {
	config.Health.CheckInterval = getEnvAsDuration("HEALTH_CHECK_INTERVAL", DefaultHealthCheckInterval)
	config.Health.MaxRetries = getEnvAsInt("HEALTH_MAX_RETRIES", DefaultHealthMaxRetries)
	config.Health.Port = getEnvAsInt("HEALTH_PORT", DefaultHealthPort)
}

// loadMetricsConfig loads metrics configuration from environment variables
func loadMetricsConfig(config *Config) {
	config.Metrics.PrintInterval = getEnvAsDuration("METRICS_PRINT_INTERVAL", DefaultMetricsPrintInterval)
}

// Environment variable utility functions
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

// Configuration getter methods with fallbacks
func (c *Config) GetESLHealthCheckTimeout() time.Duration {
	return getConfigValueWithFallback(c.ESL.HealthCheckTimeout, DefaultESLHealthCheckTimeout)
}

func (c *Config) GetESLReconnectDelay() time.Duration {
	return getConfigValueWithFallback(c.ESL.ReconnectDelay, DefaultESLReconnectDelay)
}

func (c *Config) GetESLMaxReconnectDelay() time.Duration {
	return getConfigValueWithFallback(c.ESL.MaxReconnectDelay, DefaultESLMaxReconnectDelay)
}

func (c *Config) GetRedisRemoteWriteTimeout() time.Duration {
	return getConfigValueWithFallback(c.Redis.Remote.WriteTimeout, DefaultRedisWriteTimeout)
}

func (c *Config) GetRedisRemoteDialTimeout() time.Duration {
	return getConfigValueWithFallback(c.Redis.Remote.DialTimeout, DefaultRedisDialTimeout)
}

func (c *Config) GetHealthCheckInterval() time.Duration {
	return getConfigValueWithFallback(c.Health.CheckInterval, DefaultHealthCheckInterval)
}

func (c *Config) GetHealthMaxRetries() int {
	return getConfigValueWithFallback(c.Health.MaxRetries, DefaultHealthMaxRetries)
}

func (c *Config) GetHealthPort() int {
	return getConfigValueWithFallback(c.Health.Port, DefaultHealthPort)
}

func (c *Config) GetMetricsPrintInterval() time.Duration {
	return getConfigValueWithFallback(c.Metrics.PrintInterval, DefaultMetricsPrintInterval)
}

func (c *Config) GetHealthCheckTimeout() time.Duration {
	return getConfigValueWithFallback(c.Health.CheckInterval, DefaultHealthCheckInterval)
}

// getConfigValueWithFallback returns the config value or fallback if zero
func getConfigValueWithFallback[T comparable](value, fallback T) T {
	var zero T
	if value == zero {
		return fallback
	}
	return value
}
