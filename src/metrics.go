package main

import (
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	promMessagesProcessed = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "telephony_messages_processed_total",
		Help: "Total number of processed messages.",
	})
	promErrors = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "telephony_errors_total",
		Help: "Total number of errors.",
	})
	promReaderChannelSize = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "telephony_reader_channel_size",
		Help: "Current size of the reader channel.",
	})
	promWriterChannelSize = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "telephony_writer_channel_size",
		Help: "Current size of the writer channel.",
	})
	promESLConnections = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "telephony_esl_connections_total",
		Help: "Current number of active ESL connections.",
	})
	promESLReconnections = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "telephony_esl_reconnections_total",
		Help: "Total number of ESL reconnections.",
	})
	promRedisConnections = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "telephony_redis_connections_total",
		Help: "Current number of active Redis connections.",
	})
	promRedisReconnections = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "telephony_redis_reconnections_total",
		Help: "Total number of Redis reconnections.",
	})
	promReaderLatency = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "telephony_reader_latency_milliseconds",
		Help:    "Reader latency in milliseconds.",
		Buckets: []float64{1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000},
	})
	promWriterLatency = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "telephony_writer_latency_milliseconds",
		Help:    "Writer latency in milliseconds.",
		Buckets: []float64{1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000},
	})
	promTotalLatency = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "telephony_total_latency_milliseconds",
		Help:    "Total event latency in milliseconds.",
		Buckets: []float64{1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000},
	})
)

func init() {
	prometheus.MustRegister(promMessagesProcessed)
	prometheus.MustRegister(promErrors)
	prometheus.MustRegister(promReaderChannelSize)
	prometheus.MustRegister(promWriterChannelSize)
	prometheus.MustRegister(promESLConnections)
	prometheus.MustRegister(promESLReconnections)
	prometheus.MustRegister(promRedisConnections)
	prometheus.MustRegister(promRedisReconnections)
	prometheus.MustRegister(promReaderLatency)
	prometheus.MustRegister(promWriterLatency)
	prometheus.MustRegister(promTotalLatency)
}

// MetricsManager provides thread-safe operations for the existing Metrics struct
type MetricsManager struct {
	metrics *Metrics
}

// NewMetricsManager creates a new metrics manager
func NewMetricsManager(metrics *Metrics) *MetricsManager {
	return &MetricsManager{
		metrics: metrics,
	}
}

// IncrementMessagesProcessed atomically increments the processed messages counter
func (mm *MetricsManager) IncrementMessagesProcessed() {
	atomic.AddInt64(&mm.metrics.messagesProcessed, 1)
	promMessagesProcessed.Inc()
}

// IncrementErrors atomically increments the errors counter
func (mm *MetricsManager) IncrementErrors() {
	atomic.AddInt64(&mm.metrics.errors, 1)
	promErrors.Inc()
}

// IncrementESLConnections atomically increments the ESL connections counter
func (mm *MetricsManager) IncrementESLConnections() {
	atomic.AddInt64(&mm.metrics.eslConnections, 1)
	promESLConnections.Inc()
}

// SetESLConnections sets the current number of ESL connections
func (mm *MetricsManager) SetESLConnections(count int64) {
	atomic.StoreInt64(&mm.metrics.eslConnections, count)
	promESLConnections.Set(float64(count))
}

// IncrementESLReconnections atomically increments the ESL reconnections counter
func (mm *MetricsManager) IncrementESLReconnections() {
	atomic.AddInt64(&mm.metrics.eslReconnections, 1)
	promESLReconnections.Inc()
}

// IncrementRedisConnections atomically increments the Redis connections counter
func (mm *MetricsManager) IncrementRedisConnections() {
	atomic.AddInt64(&mm.metrics.redisConnections, 1)
	promRedisConnections.Inc()
}

// SetRedisConnections sets the current number of Redis connections
func (mm *MetricsManager) SetRedisConnections(count int64) {
	atomic.StoreInt64(&mm.metrics.redisConnections, count)
	promRedisConnections.Set(float64(count))
}

// IncrementRedisReconnections atomically increments the Redis reconnections counter
func (mm *MetricsManager) IncrementRedisReconnections() {
	atomic.AddInt64(&mm.metrics.redisReconnections, 1)
	promRedisReconnections.Inc()
}

// UpdateChannelSizes updates the channel size metrics (called only when needed)
func (mm *MetricsManager) UpdateChannelSizes(readerSize, writerSize int) {
	promReaderChannelSize.Set(float64(readerSize))
	promWriterChannelSize.Set(float64(writerSize))
}

// UpdateLastSyncTime updates the last sync time
func (mm *MetricsManager) UpdateLastSyncTime() {
	mm.metrics.Lock()
	defer mm.metrics.Unlock()
	mm.metrics.lastSyncTime = time.Now()
}

// UpdateBatchMetrics updates multiple metrics in batch (more efficient)
func (mm *MetricsManager) UpdateBatchMetrics(processedCount, errorCount int64) {
	// Update counters atomically
	atomic.AddInt64(&mm.metrics.messagesProcessed, processedCount)
	atomic.AddInt64(&mm.metrics.errors, errorCount)

	// Update Prometheus metrics
	for i := int64(0); i < processedCount; i++ {
		promMessagesProcessed.Inc()
	}
	for i := int64(0); i < errorCount; i++ {
		promErrors.Inc()
	}

	// Update last sync time
	mm.UpdateLastSyncTime()
}

// ResetCounters resets all counters (useful for periodic reporting)
func (mm *MetricsManager) ResetCounters() {
	atomic.StoreInt64(&mm.metrics.messagesProcessed, 0)
	atomic.StoreInt64(&mm.metrics.errors, 0)
}

// GetSnapshot returns a thread-safe snapshot of current metrics
func (mm *MetricsManager) GetSnapshot() MetricsSnapshot {
	mm.metrics.Lock()
	defer mm.metrics.Unlock()

	return MetricsSnapshot{
		MessagesProcessed:  atomic.LoadInt64(&mm.metrics.messagesProcessed),
		Errors:             atomic.LoadInt64(&mm.metrics.errors),
		LastSyncTime:       mm.metrics.lastSyncTime,
		ReaderChannelSize:  mm.metrics.readerChannelSize,
		WriterChannelSize:  mm.metrics.writerChannelSize,
		ESLConnections:     atomic.LoadInt64(&mm.metrics.eslConnections),
		ESLReconnections:   atomic.LoadInt64(&mm.metrics.eslReconnections),
		RedisConnections:   atomic.LoadInt64(&mm.metrics.redisConnections),
		RedisReconnections: atomic.LoadInt64(&mm.metrics.redisReconnections),
	}
}

// MetricsSnapshot represents a thread-safe snapshot of metrics
type MetricsSnapshot struct {
	MessagesProcessed  int64
	Errors             int64
	LastSyncTime       time.Time
	ReaderChannelSize  int
	WriterChannelSize  int
	ESLConnections     int64
	ESLReconnections   int64
	RedisConnections   int64
	RedisReconnections int64
}

// GetMetricsManager returns the global metrics manager instance
func GetMetricsManager() *MetricsManager {
	return NewMetricsManager(metrics)
}

func ObserveReaderLatency(milliseconds float64) {
	promReaderLatency.Observe(milliseconds)
}

func ObserveWriterLatency(milliseconds float64) {
	promWriterLatency.Observe(milliseconds)
}

func ObserveTotalLatency(milliseconds float64) {
	promTotalLatency.Observe(milliseconds)
}

// printMetrics prints metrics periodically
func printMetrics() {
	config := getConfig()
	ticker := time.NewTicker(config.GetMetricsPrintInterval())
	defer ticker.Stop()

	for range ticker.C {
		metricsManager := GetMetricsManager()

		// Update channel sizes only when printing metrics (no constant updates)
		readerSize := 0
		writerSize := 0
		if globalReaderChan != nil {
			readerSize = len(globalReaderChan)
		}
		if globalWriterChan != nil {
			writerSize = len(globalWriterChan)
		}
		metricsManager.UpdateChannelSizes(readerSize, writerSize)

		snapshot := metricsManager.GetSnapshot()

		LogInfo("Messages processed (last 5s): %d, Errors: %d, Reader Channel: %d, Writer Channel: %d, Last sync: %v",
			snapshot.MessagesProcessed,
			snapshot.Errors,
			readerSize,
			writerSize,
			snapshot.LastSyncTime.Format(time.RFC3339))

		// Reset counter after each print
		metricsManager.ResetCounters()
	}
}
