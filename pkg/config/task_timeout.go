package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// TaskTimeoutEnvKey names the task timeout: how long the container of a task
// whose node declares no timeout may run, and how long the container of every
// validation, seed-build and compile Job may run. state stamps it on each
// such task's dispatch; execution-controller applies it to the release legs.
const TaskTimeoutEnvKey = "TASK_TIMEOUT_SECONDS"

// The task timeout's default and the range both services accept: the chart's
// runs.taskTimeoutHours, 1 to 168 hours.
const (
	DefaultTaskTimeout = 24 * time.Hour
	MinTaskTimeout     = time.Hour
	MaxTaskTimeout     = 168 * time.Hour
)

// LoadTaskTimeout reads TASK_TIMEOUT_SECONDS, a whole number of seconds from
// MinTaskTimeout to MaxTaskTimeout; unset selects DefaultTaskTimeout. Any other
// value is recorded on v, so the service refuses to start.
func LoadTaskTimeout(v *Validator) time.Duration {
	raw := os.Getenv(TaskTimeoutEnvKey)
	if raw == "" {
		return DefaultTaskTimeout
	}
	n, err := strconv.Atoi(raw)
	d := time.Duration(n) * time.Second
	if err != nil || d < MinTaskTimeout || d > MaxTaskTimeout {
		v.Add(fmt.Sprintf("%s (invalid value %q: expected whole seconds from %d to %d)",
			TaskTimeoutEnvKey, raw, int(MinTaskTimeout.Seconds()), int(MaxTaskTimeout.Seconds())))
		return DefaultTaskTimeout
	}
	return d
}
