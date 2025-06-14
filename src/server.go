package main

import (
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

func startHealthServer() {
	router := gin.Default()

	// Health check endpoint
	router.GET("/health", func(c *gin.Context) {
		status := healthMonitor.GetStatus()

		if healthMonitor.IsHealthy() {
			c.JSON(http.StatusOK, gin.H{
				"status": "healthy",
				"data":   status,
			})
		} else {
			c.JSON(http.StatusServiceUnavailable, gin.H{
				"status": "unhealthy",
				"data":   status,
			})
		}
	})

	// Metrics endpoint
	router.GET("/metrics", func(c *gin.Context) {
		metrics.Lock()
		defer metrics.Unlock()

		c.JSON(http.StatusOK, gin.H{
			"messages_processed":  metrics.messagesProcessed,
			"errors":              metrics.errors,
			"reader_channel_size": metrics.readerChannelSize,
			"writer_channel_size": metrics.writerChannelSize,
			"last_sync":           metrics.lastSyncTime.Format(time.RFC3339),
		})
	})

	// Start server
	addr := ":" + fmt.Sprintf("%d", healthMonitor.config.Health.Port)
	LogInfo("Starting health check server on %s", addr)
	if err := router.Run(addr); err != nil {
		LogError("Failed to start health check server: %v", err)
	}
}
