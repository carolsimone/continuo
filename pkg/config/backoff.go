package config

import (
	"math"
	"time"
)

// Backoff strategies
const (
	BackoffStrategyFixed       = "fixed"
	BackoffStrategyExponential = "exponential"
)

// Polling configuration
const (
	MaxPollAttempts        = 10
	PollingIntervalSeconds = 10
)

const (
	K8sCommandsStream = "k8s:commands"
)

// CalculateNextCheckTime calculates the next check time based on backoff strategy.
// Returns Unix timestamp as float64 (seconds since epoch).
func CalculateNextCheckTime(pollAttempts int, strategy string) float64 {
	now := float64(time.Now().Unix())

	switch strategy {
	case BackoffStrategyExponential:
		multiplier := math.Pow(2, float64(pollAttempts))
		return now + (PollingIntervalSeconds * multiplier)
	default:
		return now + PollingIntervalSeconds
	}
}
