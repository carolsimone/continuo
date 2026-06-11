package run_test

import (
	"testing"

	"github.com/carolsimone/continuo/orchestrator/domain/run"
	"github.com/stretchr/testify/assert"
)

// TestIsTerminal asserts the terminal classification over the canonical domain
// enum. Casing/stored-form translation is the neo4j adapter's responsibility
// (see run_status_codec_test.go), so a rehydrated aggregate always carries one
// of these uppercase values and IsTerminal is an exact comparison.
func TestIsTerminal(t *testing.T) {
	terminal := []run.RunStatus{
		run.RunStatusSucceeded,
		run.RunStatusFailed,
		run.RunStatusCancelled,
	}
	for _, s := range terminal {
		assert.Truef(t, s.IsTerminal(), "IsTerminal() must be true for %q", s)
	}

	nonTerminal := []run.RunStatus{
		run.RunStatusInitialized,
		run.RunStatusInProgress,
		"",
	}
	for _, s := range nonTerminal {
		assert.Falsef(t, s.IsTerminal(), "IsTerminal() must be false for %q", s)
	}
}
