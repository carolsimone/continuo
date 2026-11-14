package upstream

import (
	"context"
	"fmt"
	"path"

	"github.com/carolsimone/continuo/agent-remediation/domain/prompt"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/domain/workflow"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/agent-remediation/service/runner/step"
)

// Prompt is the prompt step `upstream`: it asks the model for the corrected
// source of the changed ancestor of an upstream cluster.
type Prompt struct {
	sanitizer        ports.LogSanitizer
	serviceRepoPaths map[string]string
}

var _ step.Prompt = (*Prompt)(nil)

// NewPrompt builds the upstream step. serviceRepoPaths maps a service name to
// its path inside the repository. It calls no port until Propose.
func NewPrompt(sanitizer ports.LogSanitizer, serviceRepoPaths map[string]string) *Prompt {
	return &Prompt{sanitizer: sanitizer, serviceRepoPaths: serviceRepoPaths}
}

// Meta describes the step.
func (*Prompt) Meta() step.Meta {
	return step.Meta{
		Name: "upstream",
		Kind: step.KindPrompt,
		Description: "Shows the model the changed ancestor's source with what this release changed in it, the failing members below it with their errors " +
			"and past precedent, and reads back the ancestor's corrected source as the draft's one edit.",
		Needs: []remediation.EvidenceKey{
			remediation.KeyFailure,
			remediation.KeyUpstreamTarget,
			remediation.KeyFailurePrecedents,
		},
	}
}

// Propose builds the request from the cluster's evidence, calls the model once
// and reads its answer into a draft of one edit of the ancestor's file. The
// source and the members' error excerpts are sanitized because they leave for an
// external model; the edit's original is the file as the bundle holds it,
// because the fix is applied to the real file. The edit's content may be empty
// or equal to the original, for the checks to refuse. A cross-service cluster
// asks the model to keep what the members read. In a round after the first the
// request is the revision of the last round's request with its answer and the
// findings (step.RevisionRequest), and the answer is read the same way.
func (p *Prompt) Propose(ctx context.Context, in step.PromptInput, m step.Model) (step.PromptOutput, error) {
	target, ok := remediation.Get[remediation.UpstreamTarget](in.Evidence, remediation.KeyUpstreamTarget)
	if !ok {
		return step.PromptOutput{}, fmt.Errorf("upstream: the evidence holds no upstream target")
	}
	fullPath, ok := repoPath(p.serviceRepoPaths, target)
	if !ok {
		return step.PromptOutput{}, fmt.Errorf("upstream: service %q has no repository path mapping", target.Service)
	}
	req, revising, err := step.RevisionRequest(in)
	if err != nil {
		return step.PromptOutput{}, fmt.Errorf("upstream: %w", err)
	}
	f := in.Failure
	if !revising {
		req = p.request(in, target)
	}

	res, err := m.Propose(ctx, req)
	if err != nil {
		return step.PromptOutput{}, fmt.Errorf("llm propose: %w", err)
	}
	base := fmt.Sprintf("proposed-fix/%s/%s/attempt-%d.source", f.Trigger.ReleaseID, target.NodeID, f.Attempt)
	return step.PromptOutput{Draft: remediation.Draft{
		Edits: []remediation.Edit{{
			Path:         fullPath,
			Original:     target.Source,
			Content:      res.ProposedSQL,
			TargetNodeID: target.NodeID,
			ContentKey:   base + ".sql",
			DiffKey:      base + ".diff",
		}},
		Rationale:      res.Rationale,
		Confidence:     res.Confidence,
		Model:          res.Model,
		SourceResolved: true,
	}}, nil
}

// request builds the first round's request from the cluster's evidence.
func (p *Prompt) request(in step.PromptInput, target remediation.UpstreamTarget) prompt.ProposeRequest {
	precedents, _ := remediation.Get[[]prompt.Precedent](in.Evidence, remediation.KeyFailurePrecedents)

	f := in.Failure
	members := make([]prompt.MemberFailure, 0, len(f.Members))
	for _, n := range f.Members {
		members = append(members, prompt.MemberFailure{NodeID: n.NodeID, Service: n.Service, ErrorExcerpt: p.sanitizer.Sanitize(n.ErrorExcerpt)})
	}
	return prompt.AssembleUpstreamFix(prompt.UpstreamEvidence{
		TargetNodeID:  target.NodeID,
		TargetSource:  p.sanitizer.Sanitize(target.Source),
		OwnChangeDiff: target.OwnChangeDiff,
		Members:       members,
		Precedents:    precedents,
		CrossService:  f.Kind == workflow.ClusterCrossService,
	})
}

// repoPath is where the ancestor's file lives in the repository.
func repoPath(serviceRepoPaths map[string]string, t remediation.UpstreamTarget) (string, bool) {
	prefix, ok := serviceRepoPaths[t.Service]
	if !ok {
		return "", false
	}
	return path.Join(prefix, t.FilePath), true
}
