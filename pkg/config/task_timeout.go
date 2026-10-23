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
// value is recorded on v, so the service refuses to start. The seconds are
// compared with the range before they become a duration, so a value too large
// for one is refused rather than wrapped into the range.
func LoadTaskTimeout(v *Validator) time.Duration {
	raw := os.Getenv(TaskTimeoutEnvKey)
	if raw == "" {
		return DefaultTaskTimeout
	}
	minSecs, maxSecs := int(MinTaskTimeout.Seconds()), int(MaxTaskTimeout.Seconds())
	n, err := strconv.Atoi(raw)
	if err != nil || n < minSecs || n > maxSecs {
		v.Add(fmt.Sprintf("%s (invalid value %q: expected whole seconds from %d to %d)",
			TaskTimeoutEnvKey, raw, minSecs, maxSecs))
		return DefaultTaskTimeout
	}
	return time.Duration(n) * time.Second
}
