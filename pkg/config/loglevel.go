package config

import (
	"fmt"
	"log/slog"
	"os"
	"slices"
	"strings"
)

// acceptedLogLevels lists the LOG_LEVEL names in the order messages and the
// chart give them; logLevels maps each to its slog level.
var acceptedLogLevels = []string{"debug", "info", "warn", "warning", "error"}

var logLevels = map[string]slog.Level{
	"debug":   slog.LevelDebug,
	"info":    slog.LevelInfo,
	"warn":    slog.LevelWarn,
	"warning": slog.LevelWarn,
	"error":   slog.LevelError,
}

// AcceptedLogLevels returns the LOG_LEVEL names every service accepts, in
// lower case; a service matches them without regard to case. The chart's
// continuo.logLevel helper (deploy/continuo/templates/_helpers.tpl) refuses
// to render a global.logLevel outside the same list.
func AcceptedLogLevels() []string {
	return slices.Clone(acceptedLogLevels)
}

// LoadLogLevel reads LOG_LEVEL, the lowest level a service writes log records
// at. Unset or empty is info. A name outside AcceptedLogLevels is recorded on
// v, so the service refuses to start, and info is returned so the caller's
// logger can report that failure.
func LoadLogLevel(v *Validator) slog.Level {
	raw := os.Getenv("LOG_LEVEL")
	if raw == "" {
		return slog.LevelInfo
	}
	level, ok := logLevels[strings.ToLower(raw)]
	if !ok {
		v.Add(fmt.Sprintf("LOG_LEVEL (invalid level %q: expected one of %s)", raw, strings.Join(acceptedLogLevels, ", ")))
		return slog.LevelInfo
	}
	return level
}
