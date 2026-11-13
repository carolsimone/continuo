package shared

import "log/slog"

// LoggerOrDefault returns l, or the process default logger when l is nil, so a
// step built with no logger still logs.
func LoggerOrDefault(l *slog.Logger) *slog.Logger {
	if l == nil {
		return slog.Default()
	}
	return l
}
