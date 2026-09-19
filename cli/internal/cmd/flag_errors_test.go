package cmd

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A flag the parser itself rejects (a non-numeric or overflowing value, or a
// flag the command does not define) must reach the caller through the same
// usage envelope and exit code as an argument the command rejects: parse
// failures happen before any command's Args validator runs, so the contract
// has to be enforced once, at the root.
func TestFlagParseFailure_EmitsUsageEnvelopeExits2(t *testing.T) {
	cases := map[string][]string{
		"non-numeric limit":  {"node", "list", "--limit", "abc"},
		"overflowing limit":  {"node", "list", "--limit", "2147483648"},
		"non-numeric offset": {"node", "list", "--offset", "-x"},
		"sibling command":    {"node", "versions", "a", "b", "c", "--limit", "abc"},
		"unknown flag":       {"node", "list", "--bogus"},
		"unknown root flag":  {"--bogus", "describe"},
	}
	for name, args := range cases {
		t.Run(name, func(t *testing.T) {
			var out, errBuf bytes.Buffer
			exit := executeWith(args, &out, &errBuf)

			assert.Equal(t, 2, exit)
			assert.Empty(t, errBuf.String(), "JSON mode keeps stderr empty")
			var env struct {
				Error struct {
					Code      string `json:"code"`
					Message   string `json:"message"`
					Retryable bool   `json:"retryable"`
				} `json:"error"`
			}
			require.NoError(t, json.Unmarshal(out.Bytes(), &env), "stdout: %q", out.String())
			assert.Equal(t, "usage", env.Error.Code)
			assert.False(t, env.Error.Retryable)
			assert.NotEmpty(t, env.Error.Message)
		})
	}
}

func TestFlagParseFailure_HumanModeWritesErrorToStderr(t *testing.T) {
	var out, errBuf bytes.Buffer
	exit := executeWith([]string{"--human", "node", "list", "--limit", "abc"}, &out, &errBuf)

	assert.Equal(t, 2, exit)
	assert.Empty(t, out.String(), "human mode keeps stdout empty")
	assert.Contains(t, errBuf.String(), "Error [usage]")
	assert.Contains(t, errBuf.String(), "limit")
}

// A command name cobra cannot resolve is rejected before any RunE, and must
// reach the caller as the same usage envelope rather than a bare exit 1.
func TestUnknownCommand_EmitsUsageEnvelopeExits2(t *testing.T) {
	cases := map[string][]string{
		"unknown top-level":  {"nope"},
		"unknown subcommand": {"node", "nope"},
	}
	for name, args := range cases {
		t.Run(name, func(t *testing.T) {
			var out, errBuf bytes.Buffer
			exit := executeWith(args, &out, &errBuf)

			assert.Equal(t, 2, exit)
			assert.Empty(t, errBuf.String())
			var env struct {
				Error struct {
					Code    string `json:"code"`
					Message string `json:"message"`
				} `json:"error"`
			}
			require.NoError(t, json.Unmarshal(out.Bytes(), &env), "stdout: %q", out.String())
			assert.Equal(t, "usage", env.Error.Code)
			assert.Contains(t, env.Error.Message, "nope")
		})
	}
}

func TestUnknownCommand_HumanModeWritesErrorToStderr(t *testing.T) {
	var out, errBuf bytes.Buffer
	exit := executeWith([]string{"--human", "nope"}, &out, &errBuf)

	assert.Equal(t, 2, exit)
	assert.Empty(t, out.String())
	assert.Contains(t, errBuf.String(), "Error [usage]")
	assert.Contains(t, errBuf.String(), "nope")
}
