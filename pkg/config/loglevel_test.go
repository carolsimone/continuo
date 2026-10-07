package config

import (
	"log/slog"
	"maps"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLoadLogLevel(t *testing.T) {
	for _, tc := range []struct {
		raw     string
		want    slog.Level
		invalid bool
	}{
		{"", slog.LevelInfo, false},
		{"debug", slog.LevelDebug, false},
		{"DEBUG", slog.LevelDebug, false},
		{"info", slog.LevelInfo, false},
		{"Info", slog.LevelInfo, false},
		{"warn", slog.LevelWarn, false},
		{"WARNING", slog.LevelWarn, false},
		{"error", slog.LevelError, false},
		{"ERROR", slog.LevelError, false},
		{"verbose", slog.LevelInfo, true},
		{"trace", slog.LevelInfo, true},
		{" info", slog.LevelInfo, true},
	} {
		t.Setenv("LOG_LEVEL", tc.raw)
		v := &Validator{}
		assert.Equal(t, tc.want, LoadLogLevel(v), tc.raw)
		assert.Equal(t, tc.invalid, len(v.Missing()) == 1, tc.raw)
	}
}

func TestLoadLogLevel_InvalidValueNamesTheAcceptedLevels(t *testing.T) {
	t.Setenv("LOG_LEVEL", "verbose")
	v := &Validator{}
	LoadLogLevel(v)
	assert.Equal(t, []string{`LOG_LEVEL (invalid level "verbose": expected one of debug, info, warn, warning, error)`}, v.Missing())
}

func TestAcceptedLogLevelsAreTheParsedOnes(t *testing.T) {
	assert.ElementsMatch(t, slices.Collect(maps.Keys(logLevels)), AcceptedLogLevels())
}
