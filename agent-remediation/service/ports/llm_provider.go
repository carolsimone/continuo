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

// ProposeResult is the structured fix-tool call result. Which fields carry the
// answer depends on the tool the request forced: a single-file fix fills
// ProposedSQL or TargetFile+ProposedContent, while a multi-file fix (a python
// node's contract) fills Files.
type ProposeResult struct {
	ProposedSQL            string
	ProposedContent        string // corrected content for the chosen TargetFile (compile/seed)
	TargetFile             string // repo-relative path the fix targets (compile/seed)
	Files                  []ProposedFile
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
