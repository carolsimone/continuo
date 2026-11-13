package contract

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/agent-remediation/service/runner/step"
	"github.com/carolsimone/continuo/agent-remediation/service/steps/shared"
)

// ContractEntry is the context source `contract_entry`: the failing node's
// normalized contract from the release's code bundle.
type ContractEntry struct {
	candidates ports.CandidateSourceReader
	sanitizer  ports.LogSanitizer
	logger     *slog.Logger
}

var _ step.ContextSource = (*ContractEntry)(nil)

// NewContractEntry builds the contract_entry step. It calls no port until
// Gather.
func NewContractEntry(candidates ports.CandidateSourceReader, sanitizer ports.LogSanitizer, logger *slog.Logger) *ContractEntry {
	return &ContractEntry{candidates: candidates, sanitizer: sanitizer, logger: shared.LoggerOrDefault(logger)}
}

// Meta describes the step.
func (*ContractEntry) Meta() step.Meta {
	return step.Meta{
		Name: "contract_entry",
		Kind: step.KindContext,
		Description: "Reads the failing python node's entry from the release's code bundle: its normalized contract as canonical JSON, sanitized. " +
			"Provides an empty text when the release has no bundle, the entry is not in it or it records another runtime.",
		Needs:    []remediation.EvidenceKey{remediation.KeyFailure},
		Provides: []remediation.EvidenceKey{remediation.KeyContractEntry},
	}
}

// Gather returns the node's entry from the release's code bundle, which is what
// the release actually parsed and validated. Here a python runtime is exactly
// what is expected, and any other runtime means the trigger and the bundle
// disagree about what this node is, so the entry is left out rather than shown
// as something it is not. A permanent miss also provides an empty text; only a
// transient fetch error is returned. An empty bundle URI means the rejection
// preceded the parse that produces the bundle (a parse-stage rejection), so
// there is no entry to look for and the reader is not consulted.
func (s *ContractEntry) Gather(ctx context.Context, in step.ContextInput) (step.ContextOutput, error) {
	f := in.Failure
	nodeID, bundleURI := f.Target.NodeID, f.Trigger.CodeBundleURI
	if bundleURI == "" {
		return entryOutput(""), nil
	}
	src, err := s.candidates.NodeSource(ctx, bundleURI, nodeID, f.Trigger.ReleaseID)
	switch {
	case err == nil && src.Runtime == ports.RuntimePython:
		return entryOutput(s.sanitizer.Sanitize(src.RawCode)), nil
	case err == nil:
		s.logger.Warn("code bundle records a non-python runtime for a python node; omitting the contract entry",
			"node", nodeID, "runtime", src.Runtime)
		return entryOutput(""), nil
	case errors.Is(err, ports.ErrNotFound):
		s.logger.Warn("contract entry not in the code bundle; proceeding without it",
			"node", nodeID, "bundle_uri", bundleURI)
		return entryOutput(""), nil
	default:
		return step.ContextOutput{}, fmt.Errorf("fetch code bundle: %w", err)
	}
}

func entryOutput(text string) step.ContextOutput {
	return step.ContextOutput{Evidence: map[remediation.EvidenceKey]any{
		remediation.KeyContractEntry: remediation.Text(text),
	}}
}
