package main

import (
	"fmt"
	"time"
)

// Error types for better error handling
type ESLConnectionError struct {
	Host string
	Port int
	Err  error
}

func (e *ESLConnectionError) Error() string {
	return fmt.Sprintf("ESL connection failed to %s:%d: %v", e.Host, e.Port, e.Err)
}

func (e *ESLConnectionError) Unwrap() error {
	return e.Err
}

type RedisConnectionError struct {
	Address string
	Err     error
}

func (e *RedisConnectionError) Error() string {
	return fmt.Sprintf("Redis connection failed to %s: %v", e.Address, e.Err)
}

func (e *RedisConnectionError) Unwrap() error {
	return e.Err
}

type ConfigurationError struct {
	Field string
	Value string
	Err   error
}

func (e *ConfigurationError) Error() string {
	return fmt.Sprintf("configuration error for field %s with value %s: %v", e.Field, e.Value, e.Err)
}

func (e *ConfigurationError) Unwrap() error {
	return e.Err
}

type PipelineError struct {
	WorkerID int
	Err      error
}

func (e *PipelineError) Error() string {
	return fmt.Sprintf("pipeline execution failed for worker %d: %v", e.WorkerID, e.Err)
}

func (e *PipelineError) Unwrap() error {
	return e.Err
}

type LatencyError struct {
	Expected time.Duration
	Actual   time.Duration
	Stage    string
}

func (e *LatencyError) Error() string {
	return fmt.Sprintf("latency exceeded in %s: expected %v, got %v", e.Stage, e.Expected, e.Actual)
}
