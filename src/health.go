package main

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/0x19/goesl"
	"github.com/redis/go-redis/v9"
)

type HealthStatus struct {
	sync.RWMutex
	lastError        error
	recoveryAttempts int
	isHealthy        bool
	lastCheck        time.Time
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
			isHealthy:        true,
			lastCheck:        time.Now(),
			recoveryAttempts: 0,
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
	if !hm.checkRedisConnections() {
		hm.status.Lock()
		hm.status.lastError = fmt.Errorf("redis connection failed")
		hm.status.isHealthy = false
		hm.status.recoveryAttempts++
		recoveryAttempts := hm.status.recoveryAttempts
		hm.status.Unlock()

		LogError("Unhealthy state detected: %v", hm.status.lastError)

		if recoveryAttempts <= hm.maxRetries {
			go hm.attemptRedisRecovery()
		} else {
			LogError("Max recovery attempts reached. Manual intervention required.")
		}
		return
	}

	// Check ESL connection
	if !hm.checkESLConnection() {
		hm.status.Lock()
		hm.status.lastError = fmt.Errorf("ESL connection failed")
		hm.status.isHealthy = false
		hm.status.recoveryAttempts++
		recoveryAttempts := hm.status.recoveryAttempts
		hm.status.Unlock()

		LogError("Unhealthy state detected: %v", hm.status.lastError)

		if recoveryAttempts <= hm.maxRetries {
			go hm.attemptESLRecovery()
		} else {
			LogError("Max recovery attempts reached. Manual intervention required.")
		}
		return
	}

	// Reset status on successful health check
	hm.status.Lock()
	hm.status.isHealthy = true
	hm.status.lastError = nil
	hm.status.recoveryAttempts = 0
	hm.status.lastCheck = time.Now()
	hm.status.Unlock()

	LogDebug("Health check passed")
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
	LogInfo("Starting Redis recovery attempt %d", hm.status.recoveryAttempts)

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
		LogError("Main system ESL client is nil")
		return false
	}

	// Test ESL client with bgapi uptime (background execution)
	ctx, cancel := context.WithTimeout(context.Background(), hm.config.GetESLHealthCheckTimeout())
	defer cancel()

	// Send bgapi uptime command and read response with timeout
	responseChan := make(chan *goesl.Message, 1)
	errChan := make(chan error, 1)

	go func() {
		// Send bgapi uptime command (background execution)
		if err := mainESLClient.client.Send("bgapi uptime"); err != nil {
			errChan <- err
			return
		}

		// Read response
		response, err := mainESLClient.client.ReadMessage()
		if err != nil {
			errChan <- err
			return
		}
		responseChan <- response
	}()

	select {
	case <-ctx.Done():
		LogError("ESL bgapi uptime timeout after %v", hm.config.GetESLHealthCheckTimeout())
		return false
	case err := <-errChan:
		LogError("ESL bgapi uptime failed: %v", err)
		return false
	case response := <-responseChan:
		// Verify response is valid
		replyText := response.GetHeader("Reply-Text")
		if replyText == "" || !strings.HasPrefix(replyText, "+OK") {
			LogError("ESL bgapi uptime response invalid: %s", replyText)
			return false
		}

		LogDebug("ESL health check passed, bgapi uptime: %s", replyText)
		return true
	}
}

func (hm *HealthMonitor) attemptESLRecovery() {
	LogInfo("Starting ESL recovery attempt %d", hm.status.recoveryAttempts)

	// Create new ESL client
	newESLClient, err := NewESLClient(hm.config)
	if err != nil {
		LogError("Failed to create new ESL client: %v", err)
		return
	}

	// Test new connection with bgapi uptime command
	ctx, cancel := context.WithTimeout(hm.ctx, hm.config.GetESLHealthCheckTimeout())
	defer cancel()

	// Test new connection with bgapi uptime command
	if err := newESLClient.client.Send("bgapi uptime"); err != nil {
		LogError("Failed to send bgapi uptime to new ESL client: %v", err)
		newESLClient.Close()
		return
	}

	// Read response with timeout
	responseChan := make(chan *goesl.Message, 1)
	errChan := make(chan error, 1)

	go func() {
		response, err := newESLClient.client.ReadMessage()
		if err != nil {
			errChan <- err
			return
		}
		responseChan <- response
	}()

	select {
	case <-ctx.Done():
		LogError("ESL recovery timeout after %v", hm.config.GetESLHealthCheckTimeout())
		newESLClient.Close()
		return
	case err := <-errChan:
		LogError("Failed to read bgapi uptime response from new ESL client: %v", err)
		newESLClient.Close()
		return
	case response := <-responseChan:
		// Verify response is valid
		replyText := response.GetHeader("Reply-Text")
		if replyText == "" || !strings.HasPrefix(replyText, "+OK") {
			LogError("New ESL client bgapi uptime response invalid: %s", replyText)
			newESLClient.Close()
			return
		}

		// Replace global ESL client (synchronize with main system)
		oldGlobalClient := GetESLClient()
		setGlobalESLClient(newESLClient)

		// Close old client
		if oldGlobalClient != nil {
			oldGlobalClient.Close()
		}

		LogInfo("Successfully reconnected to ESL, bgapi uptime: %s", replyText)
	}
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
		"is_healthy":        hm.status.isHealthy,
		"last_check":        hm.status.lastCheck,
		"last_error":        hm.status.lastError,
		"recovery_attempts": hm.status.recoveryAttempts,
	}
}
