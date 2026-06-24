package ports

import (
	"context"

	"github.com/carolsimone/continuo/remediation-agent/domain/prompt"
)

// ProposeRequest is the provider-agnostic request built by domain/prompt.
type ProposeRequest = prompt.ProposeRequest

// ProposeResult is the structured propose_fix tool call result.
type ProposeResult struct {
	ProposedSQL            string
	Rationale              string
	Confidence             string
	SuspectedRootCauseNode string
	Model                  string
}

// LLMProvider performs a single-shot, tool-forced proposal. Implementations
// translate ProposeRequest to their wire format, force the propose_fix tool,
// and parse the structured result.
type LLMProvider interface {
	Propose(ctx context.Context, req ProposeRequest) (ProposeResult, error)
}
