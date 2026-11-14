package ports

import (
	"context"

	"github.com/carolsimone/continuo/agent-remediation/domain/prompt"
)

// ProposeRequest is the provider-agnostic request built by domain/prompt.
type ProposeRequest = prompt.ProposeRequest

// ProposedFile is one complete file a multi-file fix returns: the repository
// path it changes and that file's full new content, never a diff.
type ProposedFile struct {
	Path    string
	Content string
}

// TokenUsage is the tokens one model call consumed, as the provider reports
// them: Input is the prompt side, Output the completion side.
type TokenUsage struct {
	Input  int
	Output int
}

// ProposeResult is the structured fix-tool call result. Which fields carry the
// answer depends on the tool the request forced: a single-file fix fills
// ProposedSQL or TargetFile+ProposedContent, while a multi-file fix (a python
// node's contract) fills Files. Usage is the provider-reported token count of
// the call, and Cached is true when the result was replayed from the response
// cache instead of a new model call (Usage is then the original call's).
type ProposeResult struct {
	ProposedSQL            string
	ProposedContent        string // corrected content for the chosen TargetFile (compile/seed)
	TargetFile             string // repo-relative path the fix targets (compile/seed)
	Files                  []ProposedFile
	Rationale              string
	Confidence             string
	SuspectedRootCauseNode string
	Model                  string
	Usage                  TokenUsage
	Cached                 bool
}

// LLMProvider performs a single-shot, tool-forced proposal. Implementations
// translate ProposeRequest to their wire format, force the propose_fix tool,
// and parse the structured result.
type LLMProvider interface {
	Propose(ctx context.Context, req ProposeRequest) (ProposeResult, error)
}
