package main

import (
	"runtime/debug"
)

// PanicRecovery provides a reusable panic recovery mechanism
type PanicRecovery struct {
	component string
}

// NewPanicRecovery creates a new panic recovery instance
func NewPanicRecovery(component string) *PanicRecovery {
	return &PanicRecovery{
		component: component,
	}
}

// Recover provides panic recovery with logging and metrics
func (pr *PanicRecovery) Recover() {
	if r := recover(); r != nil {
		LogError("Panic recovered in %s: %v", pr.component, r)
		GetMetricsManager().IncrementErrors()

		// Log stack trace for debugging
		LogDebug("Stack trace for %s panic: %s", pr.component, debug.Stack())
	}
}

// PanicRecoveryFunc is a simple helper for defer statements
func PanicRecoveryFunc(component string) func() {
	recovery := NewPanicRecovery(component)
	return recovery.Recover
}
