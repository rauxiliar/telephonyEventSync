package main

import (
	"net/http"
	"time"
	"log"

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
			"messages_processed": 	metrics.messagesProcessed,
			"errors":            	metrics.errors,
			"queue_size":        	metrics.queueSize,
			"latency":         	 	metrics.latency.String(),
			"last_sync":        	metrics.lastSyncTime.Format(time.RFC3339),
		})
	})

	// Start server
	if err := router.Run(":8080"); err != nil {
		log.Printf("Failed to start health check server: %v", err)
	}
}
