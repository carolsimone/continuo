package run_test

import (
	"testing"

	"github.com/carolsimone/continuo/orchestrator/domain/run"
	"github.com/stretchr/testify/assert"
)

// TestIsTerminal_AllWriterValues asserts IsTerminal() holds for every terminal
// value any of the three :Run.terminal_status writers can produce, regardless
// of casing:
//
//   - RunAggregateRepository.Save stamps the canonical lowercase form derived
//     from the in-memory uppercase aggregate status.
//   - FinalizeRun (run.finalized:v1 consumer) writes the lowercase wire values
//     state emits: "succeeded", "failed", "cancelled".
//   - the snapshot writer stamps lowercase "cancelled".
//
// IsTerminal() is case-insensitive so a rehydrated aggregate reconstructed from
// any stored value is correctly recognised as terminal.
func TestIsTerminal_AllWriterValues(t *testing.T) {
	terminal := []run.RunStatus{
		// in-memory aggregate vocabulary (uppercase)
		run.RunStatusSucceeded,
		run.RunStatusFailed,
		// canonical stored / wire forms (lowercase)
		"succeeded",
		"failed",
		"cancelled",
		// mixed casing must not slip through
		"Succeeded",
		"FAILED",
	}
	for _, s := range terminal {
		assert.Truef(t, s.IsTerminal(), "IsTerminal() must be true for %q", s)
	}

	nonTerminal := []run.RunStatus{
		run.RunStatusInitialized,
		run.RunStatusInProgress,
		"in_progress",
		"running",
		"pending",
		"",
	}
	for _, s := range nonTerminal {
		assert.Falsef(t, s.IsTerminal(), "IsTerminal() must be false for %q", s)
	}
}

// TestCanonicalTerminalStatus normalizes every terminal writer input to the
// single lowercase stored form. Non-terminal statuses normalize to the empty
// string so callers can use it as the "do not stamp" sentinel.
func TestCanonicalTerminalStatus(t *testing.T) {
	cases := map[run.RunStatus]string{
		run.RunStatusSucceeded:   "succeeded",
		run.RunStatusFailed:      "failed",
		"succeeded":              "succeeded",
		"failed":                 "failed",
		"cancelled":              "cancelled",
		"CANCELLED":              "cancelled",
		run.RunStatusInProgress:  "",
		run.RunStatusInitialized: "",
		"":                       "",
	}
	for in, want := range cases {
		assert.Equalf(t, want, in.CanonicalTerminalStatus(),
			"CanonicalTerminalStatus(%q)", in)
	}
}
