package validation

import (
	"context"
	"fmt"
	"log/slog"
	"path"
	"strings"

	"github.com/carolsimone/continuo/agent-remediation/domain/prompt"
	"github.com/carolsimone/continuo/agent-remediation/domain/proposal"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/agent-remediation/service/runner/step"
	"github.com/carolsimone/continuo/agent-remediation/service/steps/shared"
)

// sourceValidation is the trigger source of a rejection whose release
// validation ran, the only one that knows which ancestors the release changed.
const sourceValidation = "validation"

// DBTValidation is the prompt step `dbt-validation`. It asks the model for a
// fix of the failing node's candidate SQL, which is the pre-compiled SQL the
// validation ran, and then asks it to apply that diagnosis to the node's real
// source.
type DBTValidation struct {
	artifacts        ports.ArtifactWriter
	sanitizer        ports.LogSanitizer
	serviceRepoPaths map[string]string
	logger           *slog.Logger
}

var _ step.Prompt = (*DBTValidation)(nil)

// NewDBTValidation builds the dbt-validation step. serviceRepoPaths maps a
// service name to its path inside the repository. It calls no port until
// Propose.
func NewDBTValidation(artifacts ports.ArtifactWriter, sanitizer ports.LogSanitizer, serviceRepoPaths map[string]string, logger *slog.Logger) *DBTValidation {
	return &DBTValidation{artifacts: artifacts, sanitizer: sanitizer, serviceRepoPaths: serviceRepoPaths, logger: shared.LoggerOrDefault(logger)}
}

// Meta describes the step.
func (*DBTValidation) Meta() step.Meta {
	return step.Meta{
		Name: "dbt-validation",
		Kind: step.KindPrompt,
		Description: "Shows the model a validation-failed dbt node's candidate SQL with the dbt error, what the release changed in the node and its ancestors and " +
			"past precedent, and stores the SQL fix it proposes. It then asks the model to apply its diagnosis to the node's real source, " +
			"which becomes the draft's one edit when the answer is a confident change.",
		Needs: []remediation.EvidenceKey{
			remediation.KeyFailure,
			remediation.KeyCandidateSQL,
			remediation.KeyCodeBundleSource,
			remediation.KeyDBTLog,
			remediation.KeyOwnChangeDiff,
			remediation.KeyUpstreamChanges,
			remediation.KeyFailurePrecedents,
			remediation.KeyChangedAncestorDiffs,
		},
	}
}

// Propose makes the two chained model calls.
//
// The first call fixes the candidate SQL. Its answer is written as the
// candidate artifacts whenever it holds SQL, for audit: it is the model's fix
// applied to the pre-compiled SQL, not to the real model source. An answer
// with no SQL gives a draft with no artifacts and no edit.
//
// The second call applies the first one's diagnosis to the node's real source.
// It degrades quietly, to a draft with no edit, when the source or its file
// path or service is unknown, the service has no repository path, the call
// fails, its answer is empty, unchanged or rated low. On success the draft
// holds one edit of the source file. Either way the draft's confidence, model
// and suspected root are the first call's, and its rationale is the second
// call's when it produced the edit, because that note describes the diff the
// reviewer sees, and the first call's otherwise.
func (p *DBTValidation) Propose(ctx context.Context, in step.PromptInput, m step.Model) (step.PromptOutput, error) {
	candidateSQL, ok := remediation.Get[remediation.Text](in.Evidence, remediation.KeyCandidateSQL)
	if !ok {
		return step.PromptOutput{}, fmt.Errorf("dbt-validation: the evidence holds no candidate sql")
	}
	bundle, ok := remediation.Get[remediation.CandidateSource](in.Evidence, remediation.KeyCodeBundleSource)
	if !ok {
		return step.PromptOutput{}, fmt.Errorf("dbt-validation: the evidence holds no code bundle source")
	}
	dbtLog, _ := remediation.Get[remediation.Text](in.Evidence, remediation.KeyDBTLog)
	ownDiff, _ := remediation.Get[remediation.Text](in.Evidence, remediation.KeyOwnChangeDiff)
	upstream, _ := remediation.Get[[]prompt.UpstreamChange](in.Evidence, remediation.KeyUpstreamChanges)
	precedents, _ := remediation.Get[[]prompt.Precedent](in.Evidence, remediation.KeyFailurePrecedents)
	ancestors, _ := remediation.Get[[]prompt.ReleaseUpstreamChange](in.Evidence, remediation.KeyChangedAncestorDiffs)

	f := in.Failure
	nodeID := f.Target.NodeID
	res, err := m.Propose(ctx, prompt.Assemble(prompt.Evidence{
		NodeID:                 nodeID,
		ErrorSignature:         f.Target.ErrorSignature,
		CandidateSQL:           string(candidateSQL),
		DBTLog:                 string(dbtLog),
		OwnChangeDiff:          string(ownDiff),
		UpstreamChanges:        upstream,
		Precedents:             precedents,
		ReleaseUpstreamChanges: ancestors,
		ReleaseAncestorsKnown:  f.Trigger.Source == sourceValidation,
	}))
	if err != nil {
		return step.PromptOutput{}, fmt.Errorf("llm propose: %w", err)
	}
	draft := remediation.Draft{
		Rationale:     res.Rationale,
		Confidence:    res.Confidence,
		Model:         res.Model,
		SuspectedRoot: res.SuspectedRootCauseNode,
	}
	if res.ProposedSQL == "" {
		return step.PromptOutput{Draft: draft}, nil
	}

	candSQLKey := fmt.Sprintf("proposed-fix/%s/%s/attempt-%d.sql", f.Trigger.ReleaseID, nodeID, f.Attempt)
	candDiffKey := fmt.Sprintf("proposed-fix/%s/%s/attempt-%d.diff", f.Trigger.ReleaseID, nodeID, f.Attempt)
	draft.CandidateFixSQLURI, err = p.artifacts.Write(ctx, candSQLKey, res.ProposedSQL, "text/plain")
	if err != nil {
		return step.PromptOutput{}, fmt.Errorf("write candidate sql: %w", err)
	}
	draft.CandidateFixDiffURI, err = p.artifacts.Write(ctx, candDiffKey,
		proposal.ComputeUnifiedDiff(string(candidateSQL), res.ProposedSQL, nodeID), "text/plain")
	if err != nil {
		return step.PromptOutput{}, fmt.Errorf("write candidate diff: %w", err)
	}

	if edit, note, ok := p.sourceFix(ctx, f, bundle, res, m); ok {
		draft.Edits = []remediation.Edit{edit}
		draft.SourceResolved = true
		draft.Rationale = note
	}
	return step.PromptOutput{Draft: draft}, nil
}

// sourceFix asks the model to apply the first call's diagnosis to the node's
// real source, and returns the edit and the note of that call. It reports false
// on every degraded path, so the draft keeps only the candidate fix.
func (p *DBTValidation) sourceFix(ctx context.Context, f remediation.Failure, bundle remediation.CandidateSource, step1 ports.ProposeResult, m step.Model) (remediation.Edit, string, bool) {
	nodeID := f.Target.NodeID
	if bundle.Source == "" {
		p.logger.Warn("source fix: no candidate source resolved; using candidate proposal", "node", nodeID)
		return remediation.Edit{}, "", false
	}
	if bundle.FilePath == "" || bundle.Service == "" {
		p.logger.Warn("source fix: file path or service name unavailable; using candidate proposal",
			"node", nodeID, "file_path", bundle.FilePath, "service_name", bundle.Service)
		return remediation.Edit{}, "", false
	}
	repoPrefix, ok := p.serviceRepoPaths[bundle.Service]
	if !ok {
		p.logger.Warn("source fix: no repo path mapping for service; using candidate proposal",
			"node", nodeID, "service_name", bundle.Service)
		return remediation.Edit{}, "", false
	}
	out, err := m.Propose(ctx, prompt.AssembleSourceFix(p.sanitizer.Sanitize(bundle.Source), nodeID, step1.Rationale))
	if err != nil {
		p.logger.Warn("source fix: llm step 2 failed; using candidate proposal", "node", nodeID, "error", err)
		return remediation.Edit{}, "", false
	}
	if out.ProposedSQL == "" || out.ProposedSQL == bundle.Source {
		p.logger.Warn("source fix: llm step 2 produced no improvement; using candidate proposal", "node", nodeID)
		return remediation.Edit{}, "", false
	}
	if strings.EqualFold(strings.TrimSpace(out.Confidence), "low") {
		p.logger.Warn("source fix: llm step 2 low confidence; using candidate proposal", "node", nodeID)
		return remediation.Edit{}, "", false
	}
	base := fmt.Sprintf("proposed-fix/%s/%s/attempt-%d.source", f.Trigger.ReleaseID, nodeID, f.Attempt)
	return remediation.Edit{
		Path:         path.Join(repoPrefix, bundle.FilePath),
		Original:     bundle.Source,
		Content:      out.ProposedSQL,
		TargetNodeID: nodeID,
		ContentKey:   base + ".sql",
		DiffKey:      base + ".diff",
	}, out.Rationale, true
}
