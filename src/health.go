package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

type HealthStatus struct {
	sync.RWMutex
	lastHeartbeat    time.Time
	lastError        error
	recoveryAttempts int
	isHealthy        bool
	lastCheck        time.Time
}

type HealthMonitor struct {
	status          *HealthStatus
	checkInterval   time.Duration
	recoveryTimeout time.Duration
	maxRetries      int
	ctx             context.Context
	cancel          context.CancelFunc
	config          Config
	localClient     *redis.Client
	remoteClient    *redis.Client
}

func NewHealthMonitor(config Config) *HealthMonitor {
	ctx, cancel := context.WithCancel(context.Background())

	localClient := redis.NewClient(&redis.Options{
		Addr:         config.Redis.Local.Address,
		Password:     config.Redis.Local.Password,
		DB:           config.Redis.Local.DB,
		PoolSize:     config.Redis.Local.PoolSize,
		MinIdleConns: config.Redis.Local.MinIdleConns,
		MaxRetries:   config.Redis.Local.MaxRetries,
	})

	remoteClient := redis.NewClient(&redis.Options{
		Addr:         config.Redis.Remote.Address,
		Password:     config.Redis.Remote.Password,
		DB:           config.Redis.Remote.DB,
		PoolSize:     config.Redis.Remote.PoolSize,
		MinIdleConns: config.Redis.Remote.MinIdleConns,
		MaxRetries:   config.Redis.Remote.MaxRetries,
	})

	return &HealthMonitor{
		ctx:             ctx,
		cancel:          cancel,
		config:          config,
		localClient:     localClient,
		remoteClient:    remoteClient,
		checkInterval:   config.Health.CheckInterval,
		recoveryTimeout: config.Health.RecoveryTimeout,
		status: &HealthStatus{
			isHealthy:        true,
			lastCheck:        time.Now(),
			recoveryAttempts: 0,
		},
		maxRetries: config.Health.MaxRetries,
	}
}

func (hm *HealthMonitor) Start() {
	go hm.monitor()
}

func (hm *HealthMonitor) Stop() {
	hm.cancel()
}

func (hm *HealthMonitor) monitor() {
	ticker := time.NewTicker(hm.checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-hm.ctx.Done():
			return
		case <-ticker.C:
			hm.checkHealth()
		}
	}
}

func (hm *HealthMonitor) checkHealth() {
	hm.status.Lock()
	defer hm.status.Unlock()

	// Check Redis connections
	if !hm.checkRedisConnections() {
		hm.status.lastError = fmt.Errorf("Redis connections failed")
		hm.status.isHealthy = false
		hm.status.recoveryAttempts++

		log.Printf("[HEALTH] Unhealthy state detected: %v", hm.status.lastError)

		if hm.status.recoveryAttempts <= hm.maxRetries {
			go hm.attemptRecovery()
		} else {
			log.Printf("[HEALTH] Max recovery attempts reached. Manual intervention required.")
		}
		return
	}

	// Update heartbeat
	hm.status.lastHeartbeat = time.Now()
	hm.status.isHealthy = true
	hm.status.recoveryAttempts = 0
	hm.status.lastError = nil
}

func (hm *HealthMonitor) checkRedisConnections() bool {
	ctx := context.Background()

	// Check local Redis
	localAddr := hm.config.Redis.Local.Address
	if err := hm.localClient.Ping(ctx).Err(); err != nil {
		log.Printf("[HEALTH] Local Redis connection failed (%s): %v", localAddr, err)
		return false
	}

	// Check remote Redis
	remoteAddr := hm.config.Redis.Remote.Address
	if err := hm.remoteClient.Ping(ctx).Err(); err != nil {
		log.Printf("[HEALTH] Remote Redis connection failed (%s): %v", remoteAddr, err)
		return false
	}

	return true
}

func (hm *HealthMonitor) attemptRecovery() {
	log.Printf("[RECOVERY] Starting recovery attempt %d", hm.status.recoveryAttempts)

	// Create new Redis clients
	newLocal := redis.NewClient(&redis.Options{
		Addr:         hm.config.Redis.Local.Address,
		Password:     hm.config.Redis.Local.Password,
		DB:           hm.config.Redis.Local.DB,
		PoolSize:     hm.config.Redis.Local.PoolSize,
		MinIdleConns: hm.config.Redis.Local.MinIdleConns,
		MaxRetries:   hm.config.Redis.Local.MaxRetries,
	})

	newRemote := redis.NewClient(&redis.Options{
		Addr:         hm.config.Redis.Remote.Address,
		Password:     hm.config.Redis.Remote.Password,
		DB:           hm.config.Redis.Remote.DB,
		PoolSize:     hm.config.Redis.Remote.PoolSize,
		MinIdleConns: hm.config.Redis.Remote.MinIdleConns,
		MaxRetries:   hm.config.Redis.Remote.MaxRetries,
	})

	// Test new connections
	if err := newLocal.Ping(hm.ctx).Err(); err != nil {
		log.Printf("[RECOVERY] Failed to establish new local Redis connection: %v", err)
		return
	}

	if err := newRemote.Ping(hm.ctx).Err(); err != nil {
		log.Printf("[RECOVERY] Failed to establish new remote Redis connection: %v", err)
		return
	}

	// Replace old clients with new ones
	oldLocal := hm.localClient
	oldRemote := hm.remoteClient

	hm.localClient = newLocal
	hm.remoteClient = newRemote

	// Close old connections
	oldLocal.Close()
	oldRemote.Close()

	log.Printf("[RECOVERY] Successfully reconnected to Redis instances")
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
		"last_heartbeat":    hm.status.lastHeartbeat,
		"last_error":        hm.status.lastError,
		"recovery_attempts": hm.status.recoveryAttempts,
	}
}
