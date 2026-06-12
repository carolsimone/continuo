package neo4jinfra

import (
	"strings"

	domainRun "github.com/carolsimone/continuo/orchestrator/domain/run"
)

// :Run.terminal_status is persisted in a single canonical lowercase form. The
// aggregate vocabulary is the uppercase domainRun.RunStatus enum, so this
// adapter owns the translation in both directions — keeping casing and storage
// representation out of the domain layer.
//
// The three writers all stamp the lowercase form: RunAggregateRepository.Save
// (via storedTerminalStatus), FinalizeRun (the run.finalized:v1 consumer), and
// the snapshot writer ("cancelled").

// storedTerminalStatus maps a domain run status to the lowercase string stamped
// on :Run.terminal_status, or "" when the status is not terminal. The write
// boundary uses the empty string as a "do not stamp" sentinel so a non-terminal
// aggregate never overwrites a terminal projection.
func storedTerminalStatus(s domainRun.RunStatus) string {
	switch s {
	case domainRun.RunStatusSucceeded:
		return "succeeded"
	case domainRun.RunStatusFailed:
		return "failed"
	case domainRun.RunStatusCancelled:
		return "cancelled"
	default:
		return ""
	}
}

// runStatusFromStored maps a stored (or wire) terminal_status value back into
// the domain enum during rehydration. It is case-insensitive so a value written
// by any producer — or a legacy uppercase row predating casing normalization —
// resolves to the canonical aggregate status. An unrecognized value yields ""
// (the zero RunStatus), which callers treat as "no terminal status".
func runStatusFromStored(stored string) domainRun.RunStatus {
	switch strings.ToLower(stored) {
	case "succeeded":
		return domainRun.RunStatusSucceeded
	case "failed":
		return domainRun.RunStatusFailed
	case "cancelled":
		return domainRun.RunStatusCancelled
	default:
		return ""
	}
}
