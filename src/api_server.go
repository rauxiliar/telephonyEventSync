package main

import (
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func startAPIServer() {
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

	// Metrics endpoint (Prometheus)
	router.GET("/metrics", gin.WrapH(promhttp.Handler()))

	// Metrics endpoint
	router.GET("/metrics", func(c *gin.Context) {
		metricsManager := GetMetricsManager()
		snapshot := metricsManager.GetSnapshot()

		c.JSON(http.StatusOK, gin.H{
			"messages_processed":  snapshot.MessagesProcessed,
			"errors":              snapshot.Errors,
			"reader_channel_size": snapshot.ReaderChannelSize,
			"writer_channel_size": snapshot.WriterChannelSize,
			"esl_connections":     snapshot.ESLConnections,
			"esl_reconnections":   snapshot.ESLReconnections,
			"last_sync":           snapshot.LastSyncTime.Format(time.RFC3339),
		})
	})

	// Start server
	addr := ":" + fmt.Sprintf("%d", healthMonitor.config.GetHealthPort())
	LogInfo("Starting API server on %s", addr)
	if err := router.Run(addr); err != nil {
		LogError("Failed to start API server: %v", err)
	}
}
