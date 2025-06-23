package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
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

func (hm *HealthMonitor) monitor() {
	for {
		select {
		case <-hm.ctx.Done():
			return
		case <-hm.ticker.C:
			hm.performHealthCheck()
		}
	}
}

func (hm *HealthMonitor) performHealthCheck() {
	// Check Redis connections
	redisHealthy := hm.checkRedisConnections()
	if !redisHealthy {
		hm.status.Lock()
		hm.status.lastError = fmt.Errorf("redis connection failed")
		hm.status.isHealthy = false
		hm.status.redisRecoveryAttempts++
		hm.status.Unlock()

		LogError("Unhealthy state detected: %v", hm.status.lastError)

		if hm.status.redisRecoveryAttempts <= hm.maxRetries {
			go hm.attemptRedisRecovery()
		} else {
			LogError("Max Redis recovery attempts reached. Manual intervention required.")
		}
	} else {
		// Reset Redis recovery attempts if Redis is healthy
		hm.status.Lock()
		hm.status.redisRecoveryAttempts = 0
		hm.status.Unlock()
	}

	// Check ESL connection
	eslHealthy := hm.checkESLConnection()
	if !eslHealthy {
		hm.status.Lock()
		hm.status.lastError = fmt.Errorf("ESL connection failed")
		hm.status.isHealthy = false
		hm.status.eslRecoveryAttempts++
		hm.status.Unlock()

		LogError("Unhealthy state detected: %v", hm.status.lastError)

		// ESL recovery is handled by the main connection loop
		// No need for separate recovery attempt here
	} else {
		// Reset ESL recovery attempts if ESL is healthy
		hm.status.Lock()
		hm.status.eslRecoveryAttempts = 0
		hm.status.Unlock()
	}

	// Overall health status
	if redisHealthy && eslHealthy {
		hm.status.Lock()
		hm.status.isHealthy = true
		hm.status.lastError = nil
		hm.status.lastCheck = time.Now()
		hm.status.Unlock()

		LogDebug("Health check passed")
	}
}

func (hm *HealthMonitor) checkRedisConnections() bool {
	// Use shorter timeout for faster failure detection
	ctx, cancel := context.WithTimeout(context.Background(), hm.config.Redis.Remote.DialTimeout)
	defer cancel()

	// Check health monitor Redis client
	remoteAddr := hm.config.Redis.Remote.Address
	if err := rRemote.Ping(ctx).Err(); err != nil {
		LogError("Redis connection failed (%s): %v", remoteAddr, err)
		return false
	}

	LogDebug("Redis health check passed")
	return true
}

func (hm *HealthMonitor) attemptRedisRecovery() {
	LogInfo("Starting Redis recovery attempt %d", hm.status.redisRecoveryAttempts)

	newRemote := redis.NewClient(&redis.Options{
		Addr:         hm.config.Redis.Remote.Address,
		Password:     hm.config.Redis.Remote.Password,
		DB:           hm.config.Redis.Remote.DB,
		PoolSize:     hm.config.Redis.Remote.PoolSize,
		MinIdleConns: hm.config.Redis.Remote.MinIdleConns,
		MaxRetries:   hm.config.Redis.Remote.MaxRetries,
		// Use configured timeouts for recovery
		DialTimeout:  hm.config.Redis.Remote.DialTimeout,
		WriteTimeout: hm.config.Redis.Remote.WriteTimeout,
		PoolTimeout:  hm.config.Redis.Remote.PoolTimeout,
	})

	// Test new connections with timeout
	ctx, cancel := context.WithTimeout(hm.ctx, hm.config.Redis.Remote.DialTimeout)
	defer cancel()

	if err := newRemote.Ping(ctx).Err(); err != nil {
		LogError("Failed to establish new remote Redis connection: %v", err)
		return
	}

	// Replace global Redis client (synchronize with main system)
	oldGlobalRemote := rRemote
	rRemote = newRemote

	// Close old connections
	if oldGlobalRemote != nil {
		oldGlobalRemote.Close()
	}

	LogInfo("Successfully reconnected to Redis instances")
}

func (hm *HealthMonitor) checkESLConnection() bool {
	// Check main system ESL client
	mainESLClient := GetESLClient()
	if mainESLClient == nil {
		LogError("ESL client is nil")
		return false
	}

	// Simple connection check - just verify client exists and is not nil
	// The main ESL connection loop will handle reconnection if needed
	LogDebug("ESL health check passed")
	return true
}

func (hm *HealthMonitor) IsHealthy() bool {
	hm.status.RLock()
	defer hm.status.RUnlock()
	return hm.status.isHealthy
}

func (hm *HealthMonitor) GetStatus() map[string]any {
	hm.status.RLock()
	defer hm.status.RUnlock()

	return map[string]any{
		"is_healthy":              hm.status.isHealthy,
		"last_check":              hm.status.lastCheck,
		"last_error":              hm.status.lastError,
		"redis_recovery_attempts": hm.status.redisRecoveryAttempts,
		"esl_recovery_attempts":   hm.status.eslRecoveryAttempts,
	}
}
