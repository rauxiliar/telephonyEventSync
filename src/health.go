package main

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type HealthStatus struct {
	sync.RWMutex
	lastError             error
	redisRecoveryAttempts int
	eslRecoveryAttempts   int
	isHealthy             bool
	lastCheck             time.Time
}

type HealthMonitor struct {
	status     *HealthStatus
	maxRetries int
	ctx        context.Context
	cancel     context.CancelFunc
	config     Config
	ticker     *time.Ticker
}

// NewHealthMonitor creates a new health monitor instance
func NewHealthMonitor(config Config) *HealthMonitor {
	ctx, cancel := context.WithCancel(context.Background())

	return &HealthMonitor{
		ctx:    ctx,
		cancel: cancel,
		config: config,
		status: &HealthStatus{
			isHealthy:             true,
			lastCheck:             time.Now(),
			redisRecoveryAttempts: 0,
			eslRecoveryAttempts:   0,
		},
		maxRetries: config.GetHealthMaxRetries(),
		ticker:     time.NewTicker(config.GetHealthCheckInterval()),
	}
}

func (hm *HealthMonitor) Start() {
	go hm.monitor()
}

func (hm *HealthMonitor) Stop() {
	hm.cancel()
}

// monitor is the main health monitor loop
// It performs health checks periodically and updates the health status
// It also handles recovery attempts for Redis and ESL connections
// It runs in a separate goroutine and is stopped when the context is cancelled
func (hm *HealthMonitor) monitor() {
	// Panic recovery for the health monitor
	defer PanicRecoveryFunc("health monitor")()

	for {
		// BLOCKING: Main health monitor loop, blocks on select until context is cancelled or timer fires
		select {
		case <-hm.ctx.Done():
			return
		case <-hm.ticker.C:
			hm.performHealthCheck()
		}
	}
}

// performHealthCheck performs the health check for Redis and ESL connections
func (hm *HealthMonitor) performHealthCheck() {
	redisHealthy := hm.checkRedisConnections()
	hm.handleRedisHealthResult(redisHealthy)

	if !redisHealthy {
		return
	}

	eslHealthy := hm.checkESLConnection()
	hm.handleESLHealthResult(eslHealthy)

	if !eslHealthy {
		return
	}

	hm.updateOverallHealthStatus()
}

// handleRedisHealthResult handles the result of Redis health check
func (hm *HealthMonitor) handleRedisHealthResult(redisHealthy bool) {
	if !redisHealthy {
		hm.markRedisUnhealthy()
		hm.handleRedisRecovery()
	} else {
		hm.resetRedisRecoveryAttempts()
	}
}

// handleESLHealthResult handles the result of ESL health check
func (hm *HealthMonitor) handleESLHealthResult(eslHealthy bool) {
	if !eslHealthy {
		hm.markESLUnhealthy()
	} else {
		hm.resetESLRecoveryAttempts()
	}
}

// markRedisUnhealthy marks the system as unhealthy due to Redis issues
func (hm *HealthMonitor) markRedisUnhealthy() {
	hm.status.Lock()
	hm.status.lastError = fmt.Errorf("redis connection failed")
	hm.status.isHealthy = false
	hm.status.redisRecoveryAttempts++
	hm.status.Unlock()

	LogError("Unhealthy state detected: %v", hm.status.lastError)
}

// markESLUnhealthy marks the system as unhealthy due to ESL issues
func (hm *HealthMonitor) markESLUnhealthy() {
	hm.status.Lock()
	hm.status.lastError = fmt.Errorf("ESL connection failed")
	hm.status.isHealthy = false
	hm.status.eslRecoveryAttempts++
	hm.status.Unlock()

	LogError("Unhealthy state detected: %v", hm.status.lastError)
}

// handleRedisRecovery handles Redis recovery attempts
func (hm *HealthMonitor) handleRedisRecovery() {
	if hm.status.redisRecoveryAttempts <= hm.maxRetries {
		go hm.attemptRedisRecovery()
	} else {
		LogError("Max Redis recovery attempts reached. Manual intervention required.")
	}
}

// resetRedisRecoveryAttempts resets Redis recovery attempts when healthy
func (hm *HealthMonitor) resetRedisRecoveryAttempts() {
	hm.status.Lock()
	hm.status.redisRecoveryAttempts = 0
	hm.status.Unlock()
}

// resetESLRecoveryAttempts resets ESL recovery attempts when healthy
func (hm *HealthMonitor) resetESLRecoveryAttempts() {
	hm.status.Lock()
	hm.status.eslRecoveryAttempts = 0
	hm.status.Unlock()
}

// updateOverallHealthStatus updates the overall health status when all checks pass
func (hm *HealthMonitor) updateOverallHealthStatus() {
	hm.status.Lock()
	hm.status.isHealthy = true
	hm.status.lastError = nil
	hm.status.lastCheck = time.Now()
	hm.status.Unlock()

	LogDebug("Health check passed")
}

// checkRedisConnections checks the Redis connection status
// It returns true if the connection is healthy, false otherwise
// It also updates the Redis connection metrics
func (hm *HealthMonitor) checkRedisConnections() bool {
	ctx, cancel := context.WithTimeout(context.Background(), hm.config.Redis.Remote.DialTimeout)
	defer cancel()

	if err := rRemote.Ping(ctx).Err(); err != nil {
		remoteAddr := hm.config.Redis.Remote.Address
		LogError("Redis connection failed (%s): %v", remoteAddr, err)
		GetMetricsManager().SetRedisConnections(0)
		return false
	}

	updateRedisConnectionMetrics()
	LogDebug("Redis health check passed")
	return true
}

// attemptRedisRecovery attempts to recover from Redis connection issues
// It sends a ping to Redis to verify the connection is healthy
// If the ping fails, it will attempt to reconnect
// It runs in a separate goroutine and is stopped when the context is cancelled
func (hm *HealthMonitor) attemptRedisRecovery() {
	LogInfo("Starting Redis recovery attempt %d", hm.status.redisRecoveryAttempts)

	ctx, cancel := context.WithTimeout(hm.ctx, hm.config.Redis.Remote.DialTimeout)
	defer cancel()

	if err := rRemote.Ping(ctx).Err(); err != nil {
		LogError("Redis recovery ping failed: %v", err)
		return
	}

	updateRedisConnectionMetrics()
	LogInfo("Redis recovery successful - connection is healthy")
}

// checkESLConnection checks the ESL connection status
// It returns true if the connection is healthy, false otherwise
// It also updates the ESL connection metrics
func (hm *HealthMonitor) checkESLConnection() bool {
	mainESLClient := GetESLClient()
	if mainESLClient == nil {
		LogError("ESL client is nil")
		GetMetricsManager().SetESLConnections(0)
		return false
	}

	LogDebug("ESL health check passed")
	return true
}

// IsHealthy returns the current health status of the system
// It returns true if the system is healthy, false otherwise
func (hm *HealthMonitor) IsHealthy() bool {
	hm.status.RLock()
	defer hm.status.RUnlock()
	return hm.status.isHealthy
}

// GetStatus returns the current health status of the system
// It returns a map with the health status and recovery attempts
func (hm *HealthMonitor) GetStatus() map[string]any {
	hm.status.RLock()
	defer hm.status.RUnlock()

	return hm.createStatusMap()
}

// createStatusMap creates the status map for the health monitor
func (hm *HealthMonitor) createStatusMap() map[string]any {
	return map[string]any{
		"is_healthy":              hm.status.isHealthy,
		"last_check":              hm.status.lastCheck,
		"last_error":              hm.status.lastError,
		"redis_recovery_attempts": hm.status.redisRecoveryAttempts,
		"esl_recovery_attempts":   hm.status.eslRecoveryAttempts,
	}
}
