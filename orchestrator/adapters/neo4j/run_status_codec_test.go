package neo4jinfra

import (
	"testing"

	domainRun "github.com/carolsimone/continuo/orchestrator/domain/run"
	"github.com/stretchr/testify/assert"
)

// TestStoredTerminalStatus pins the write boundary: every terminal domain
// status maps to its canonical lowercase stored form, and non-terminal statuses
// map to "" so they are never stamped onto :Run.terminal_status.
func TestStoredTerminalStatus(t *testing.T) {
	cases := map[domainRun.RunStatus]string{
		domainRun.RunStatusSucceeded:   "succeeded",
		domainRun.RunStatusFailed:      "failed",
		domainRun.RunStatusCancelled:   "cancelled",
		domainRun.RunStatusInProgress:  "",
		domainRun.RunStatusInitialized: "",
		"":                             "",
	}
	for in, want := range cases {
		assert.Equalf(t, want, storedTerminalStatus(in), "storedTerminalStatus(%q)", in)
	}
}

// TestRunStatusFromStored proves rehydration resolves every value any writer can
// leave in :Run.terminal_status back to the canonical uppercase domain enum,
// regardless of casing — the lowercase forms written by Save/FinalizeRun/the
// snapshot writer, plus legacy uppercase rows predating normalization. A
// resolved status must satisfy domain IsTerminal(); unknown input yields "".
func TestRunStatusFromStored(t *testing.T) {
	cases := map[string]domainRun.RunStatus{
		"succeeded": domainRun.RunStatusSucceeded,
		"failed":    domainRun.RunStatusFailed,
		"cancelled": domainRun.RunStatusCancelled,
		// legacy / mixed casing must still resolve
		"SUCCEEDED": domainRun.RunStatusSucceeded,
		"Failed":    domainRun.RunStatusFailed,
		// non-terminal / unknown
		"in_progress": "",
		"":            "",
	}
	for in, want := range cases {
		got := runStatusFromStored(in)
		assert.Equalf(t, want, got, "runStatusFromStored(%q)", in)
		if want != "" {
			assert.Truef(t, got.IsTerminal(), "resolved %q (%q) must be terminal", in, got)
		}
	}
}

func TestRunStatusCodec_Skipped_RoundTrips(t *testing.T) {
	if got := storedTerminalStatus(domainRun.RunStatusSkipped); got != "skipped" {
		t.Fatalf("storedTerminalStatus(skipped)=%q, want %q", got, "skipped")
	}
	if got := runStatusFromStored("skipped"); got != domainRun.RunStatusSkipped {
		t.Fatalf("runStatusFromStored(skipped)=%q, want RunStatusSkipped", got)
	}
	if got := runStatusFromStored("SKIPPED"); got != domainRun.RunStatusSkipped {
		t.Fatalf("case-insensitive: runStatusFromStored(SKIPPED)=%q, want RunStatusSkipped", got)
	}
}
