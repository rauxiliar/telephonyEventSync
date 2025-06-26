package main

import (
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// It starts the API server
// It creates a new Gin router
// It adds a health check endpoint
// It adds a metrics endpoint (Prometheus)
// It starts the server on the configured port
// It logs the server start message
// It logs the server error if it fails to start
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

	// Start server
	addr := ":" + fmt.Sprintf("%d", healthMonitor.config.GetHealthPort())
	LogInfo("Starting API server on %s", addr)
	if err := router.Run(addr); err != nil {
		LogError("Failed to start API server: %v", err)
	}
}
