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

// MetricsManager provides thread-safe operations for the existing Metrics struct
type MetricsManager struct {
	metrics *Metrics
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

func init() {
	registerPrometheusMetrics()
}

// registerPrometheusMetrics registers all Prometheus metrics
func registerPrometheusMetrics() {
	registerCounterMetrics()
	registerGaugeMetrics()
	registerHistogramMetrics()
}

// registerCounterMetrics registers counter-type Prometheus metrics
func registerCounterMetrics() {
	prometheus.MustRegister(promMessagesProcessed)
	prometheus.MustRegister(promErrors)
	prometheus.MustRegister(promESLReconnections)
	prometheus.MustRegister(promRedisReconnections)
}

// registerGaugeMetrics registers gauge-type Prometheus metrics
func registerGaugeMetrics() {
	prometheus.MustRegister(promReaderChannelSize)
	prometheus.MustRegister(promWriterChannelSize)
	prometheus.MustRegister(promESLConnections)
	prometheus.MustRegister(promRedisConnections)
}

// registerHistogramMetrics registers histogram-type Prometheus metrics
func registerHistogramMetrics() {
	prometheus.MustRegister(promReaderLatency)
	prometheus.MustRegister(promWriterLatency)
	prometheus.MustRegister(promTotalLatency)
}

// IncrementMessagesProcessed atomically increments the processed messages counter
func (mm *MetricsManager) IncrementMessagesProcessed() {
	mm.incrementCounter(&mm.metrics.messagesProcessed, promMessagesProcessed)
}

// IncrementErrors atomically increments the errors counter
func (mm *MetricsManager) IncrementErrors() {
	mm.incrementCounter(&mm.metrics.errors, promErrors)
}

// IncrementESLConnections atomically increments the ESL connections counter
func (mm *MetricsManager) IncrementESLConnections() {
	mm.incrementCounter(&mm.metrics.eslConnections, promESLConnections)
}

// SetESLConnections sets the current number of ESL connections
func (mm *MetricsManager) SetESLConnections(count int64) {
	mm.setGauge(&mm.metrics.eslConnections, promESLConnections, count)
}

// IncrementESLReconnections atomically increments the ESL reconnections counter
func (mm *MetricsManager) IncrementESLReconnections() {
	mm.incrementCounter(&mm.metrics.eslReconnections, promESLReconnections)
}

// IncrementRedisConnections atomically increments the Redis connections counter
func (mm *MetricsManager) IncrementRedisConnections() {
	mm.incrementCounter(&mm.metrics.redisConnections, promRedisConnections)
}

// SetRedisConnections sets the current number of Redis connections
func (mm *MetricsManager) SetRedisConnections(count int64) {
	mm.setGauge(&mm.metrics.redisConnections, promRedisConnections, count)
}

// IncrementRedisReconnections atomically increments the Redis reconnections counter
func (mm *MetricsManager) IncrementRedisReconnections() {
	mm.incrementCounter(&mm.metrics.redisReconnections, promRedisReconnections)
}

// incrementCounter atomically increments both internal and Prometheus counters
func (mm *MetricsManager) incrementCounter(internalCounter *int64, promCounter prometheus.Counter) {
	atomic.AddInt64(internalCounter, 1)
	promCounter.Inc()
}

// setGauge atomically sets both internal and Prometheus gauge values
func (mm *MetricsManager) setGauge(internalGauge *int64, promGauge prometheus.Gauge, value int64) {
	atomic.StoreInt64(internalGauge, value)
	promGauge.Set(float64(value))
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
	mm.updateBatchCounters(processedCount, errorCount)
	mm.UpdateLastSyncTime()
}

// updateBatchCounters updates counters in batch
func (mm *MetricsManager) updateBatchCounters(processedCount, errorCount int64) {
	// Update counters atomically
	atomic.AddInt64(&mm.metrics.messagesProcessed, processedCount)
	atomic.AddInt64(&mm.metrics.errors, errorCount)

	// Update Prometheus metrics
	mm.updatePrometheusBatchMetrics(processedCount, errorCount)
}

// updatePrometheusBatchMetrics updates Prometheus metrics in batch
func (mm *MetricsManager) updatePrometheusBatchMetrics(processedCount, errorCount int64) {
	if processedCount > 0 {
		promMessagesProcessed.Add(float64(processedCount))
	}
	if errorCount > 0 {
		promErrors.Add(float64(errorCount))
	}
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

	return mm.createMetricsSnapshot()
}

// createMetricsSnapshot creates a snapshot of current metrics
func (mm *MetricsManager) createMetricsSnapshot() MetricsSnapshot {
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

// GetMetricsManager returns the global metrics manager instance
func GetMetricsManager() *MetricsManager {
	return &MetricsManager{
		metrics: metrics,
	}
}

// printMetrics prints metrics periodically
func printMetrics() {
	config := getConfig()
	metricsInterval := config.GetMetricsPrintInterval()
	ticker := time.NewTicker(metricsInterval)
	defer ticker.Stop()

	for range ticker.C {
		processMetricsPrinting()
	}
}

// processMetricsPrinting handles the periodic metrics printing
func processMetricsPrinting() {
	metricsManager := GetMetricsManager()
	updateChannelSizes(metricsManager)
	printMetricsSnapshot(metricsManager)
	metricsManager.ResetCounters()
}

// updateChannelSizes updates channel size metrics
func updateChannelSizes(metricsManager *MetricsManager) {
	var readerSize, writerSize int
	if globalReaderChan != nil {
		readerSize = len(globalReaderChan)
	}
	if globalWriterChan != nil {
		writerSize = len(globalWriterChan)
	}
	metricsManager.UpdateChannelSizes(readerSize, writerSize)
}

// printMetricsSnapshot prints the current metrics snapshot
func printMetricsSnapshot(metricsManager *MetricsManager) {
	config := getConfig()
	metricsInterval := config.GetMetricsPrintInterval()
	snapshot := metricsManager.GetSnapshot()
	var readerSize, writerSize int
	if globalReaderChan != nil {
		readerSize = len(globalReaderChan)
	}
	if globalWriterChan != nil {
		writerSize = len(globalWriterChan)
	}

	LogInfo("Messages processed (last %v): %d, Errors: %d, Reader Channel: %d, Writer Channel: %d, Last sync: %v",
		metricsInterval,
		snapshot.MessagesProcessed,
		snapshot.Errors,
		readerSize,
		writerSize,
		snapshot.LastSyncTime.Format(time.RFC3339))
}
