package validation

import (
	"context"
	"fmt"
	"path"

	"github.com/carolsimone/continuo/agent-remediation/domain/prompt"
	"github.com/carolsimone/continuo/agent-remediation/domain/proposal"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/agent-remediation/service/runner/step"
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
}

var _ step.Prompt = (*DBTValidation)(nil)

// NewDBTValidation builds the dbt-validation step. serviceRepoPaths maps a
// service name to its path inside the repository. It calls no port until
// Propose.
func NewDBTValidation(artifacts ports.ArtifactWriter, sanitizer ports.LogSanitizer, serviceRepoPaths map[string]string) *DBTValidation {
	return &DBTValidation{artifacts: artifacts, sanitizer: sanitizer, serviceRepoPaths: serviceRepoPaths}
}

// Meta describes the step.
func (*DBTValidation) Meta() step.Meta {
	return step.Meta{
		Name: "dbt-validation",
		Kind: step.KindPrompt,
		Description: "Shows the model a validation-failed dbt node's candidate SQL with the dbt error, what the release changed in the node and its ancestors and " +
			"past precedent, and stores the SQL fix it proposes. It then asks the model to apply its diagnosis to the node's real source, " +
			"and the answer, whatever it holds, becomes the draft's one edit for the pre-flight checks to judge. " +
			"Both requests of the first round carry the earlier tries at the failure, when there are any.",
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

// Propose makes the two chained model calls of the first round, and in a round
// after the first revises the last of them.
//
// The first call fixes the candidate SQL. Its answer is written as the
// candidate artifacts whenever it holds SQL, for audit: it is the model's fix
// applied to the pre-compiled SQL, not to the real model source. An answer
// with no SQL gives a draft with no artifacts and no edit.
//
// Both calls of the first round carry the earlier tries at the failure
// (step.WithPreviousTries), so whichever of them a later round revises still
// shows them.
//
// The second call applies the first one's diagnosis to the node's real source.
// Its answer is the draft's one edit whatever it holds, an empty, unchanged or
// low-confidence one included: the pre-flight checks judge it, and a round
// after the first revises it. A failed call is the step's error, which the
// runner retries. The draft's rationale and confidence are the second call's,
// because that note describes the diff the reviewer sees and that confidence
// rates it, and its model and suspected root are the first call's. The source
// is always reachable: code_bundle_source declines a node whose source, file
// path, service or service's repository path is unknown, and evidence that
// lacks one is an error that calls no model.
//
// A round after the first revises the last call of the round before it, with
// its answer and the findings (step.RevisionRequest). When that call is the
// source fix, the one call of the round is its revision, read as the second
// call is, and the diagnosis is not made again: the draft keeps the previous
// draft's candidate fix, model and suspected root. When it is the diagnosis,
// which is all a first round makes when the answer holds no SQL, the diagnosis
// is revised and its answer goes the way a first call's does, including the
// source fix.
func (p *DBTValidation) Propose(ctx context.Context, in step.PromptInput, m step.Model) (step.PromptOutput, error) {
	candidateSQL, ok := remediation.Get[remediation.Text](in.Evidence, remediation.KeyCandidateSQL)
	if !ok {
		return step.PromptOutput{}, fmt.Errorf("dbt-validation: the evidence holds no candidate sql")
	}
	bundle, ok := remediation.Get[remediation.CandidateSource](in.Evidence, remediation.KeyCodeBundleSource)
	if !ok {
		return step.PromptOutput{}, fmt.Errorf("dbt-validation: the evidence holds no code bundle source")
	}
	repoPrefix, err := p.repoPrefix(bundle)
	if err != nil {
		return step.PromptOutput{}, err
	}
	req, revising, err := step.RevisionRequest(in)
	if err != nil {
		return step.PromptOutput{}, fmt.Errorf("dbt-validation: %w", err)
	}
	if revising && prompt.IsSourceFix(req) {
		return p.reviseSourceFix(ctx, in, bundle, repoPrefix, req, m)
	}
	if !revising {
		req = p.diagnosisRequest(in, candidateSQL)
	}

	f := in.Failure
	nodeID := f.Target.NodeID
	res, err := m.Propose(ctx, req)
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

	applyDiagnosis := step.WithPreviousTries(prompt.AssembleSourceFix(p.sanitizer.Sanitize(bundle.Source), nodeID, res.Rationale), in.Evidence)
	return p.sourceFix(ctx, f, bundle, repoPrefix, applyDiagnosis, draft, m)
}

// repoPrefix is the repository path of the service the node's source lives in.
// code_bundle_source gives a node only when its source, file path and service
// are known and the service has a repository path, so evidence without one of
// them is an error.
func (p *DBTValidation) repoPrefix(bundle remediation.CandidateSource) (string, error) {
	if bundle.Source == "" || bundle.FilePath == "" || bundle.Service == "" {
		return "", fmt.Errorf("dbt-validation: the code bundle source holds no source, file path or service to edit")
	}
	prefix, ok := p.serviceRepoPaths[bundle.Service]
	if !ok {
		return "", fmt.Errorf("dbt-validation: service %q has no repository path mapping", bundle.Service)
	}
	return prefix, nil
}

// diagnosisRequest builds the first call's request from the cluster's
// evidence, with the earlier tries at the failure (step.WithPreviousTries).
func (p *DBTValidation) diagnosisRequest(in step.PromptInput, candidateSQL remediation.Text) prompt.ProposeRequest {
	dbtLog, _ := remediation.Get[remediation.Text](in.Evidence, remediation.KeyDBTLog)
	ownDiff, _ := remediation.Get[remediation.Text](in.Evidence, remediation.KeyOwnChangeDiff)
	upstream, _ := remediation.Get[[]prompt.UpstreamChange](in.Evidence, remediation.KeyUpstreamChanges)
	precedents, _ := remediation.Get[[]prompt.Precedent](in.Evidence, remediation.KeyFailurePrecedents)
	ancestors, _ := remediation.Get[[]prompt.ReleaseUpstreamChange](in.Evidence, remediation.KeyChangedAncestorDiffs)

	f := in.Failure
	return step.WithPreviousTries(prompt.Assemble(prompt.Evidence{
		NodeID:                 f.Target.NodeID,
		ErrorSignature:         f.Target.ErrorSignature,
		CandidateSQL:           string(candidateSQL),
		DBTLog:                 string(dbtLog),
		OwnChangeDiff:          string(ownDiff),
		UpstreamChanges:        upstream,
		Precedents:             precedents,
		ReleaseUpstreamChanges: ancestors,
		ReleaseAncestorsKnown:  f.Trigger.Source == sourceValidation,
	}), in.Evidence)
}

// reviseSourceFix makes the one call of a round that revises the previous
// round's source fix, and reads its answer as the second call is read. The
// draft is the previous draft's candidate fix, model and suspected root, which
// came from the first call, with the revised answer as its edit, note and
// confidence.
func (p *DBTValidation) reviseSourceFix(ctx context.Context, in step.PromptInput, bundle remediation.CandidateSource, repoPrefix string, revision prompt.ProposeRequest, m step.Model) (step.PromptOutput, error) {
	if in.Previous == nil {
		return step.PromptOutput{}, fmt.Errorf("dbt-validation: a revision of the source fix needs the previous round's draft")
	}
	prev := *in.Previous
	draft := remediation.Draft{
		Model:               prev.Model,
		SuspectedRoot:       prev.SuspectedRoot,
		CandidateFixSQLURI:  prev.CandidateFixSQLURI,
		CandidateFixDiffURI: prev.CandidateFixDiffURI,
	}
	return p.sourceFix(ctx, in.Failure, bundle, repoPrefix, revision, draft, m)
}

// sourceFix makes the source-fix call with request and returns draft with the
// call's answer as its one edit of the source file, and the answer's note and
// confidence as the draft's. The answer is the edit however little it holds, for
// the checks to judge. A failed call is an error.
func (p *DBTValidation) sourceFix(ctx context.Context, f remediation.Failure, bundle remediation.CandidateSource, repoPrefix string, request prompt.ProposeRequest, draft remediation.Draft, m step.Model) (step.PromptOutput, error) {
	out, err := m.Propose(ctx, request)
	if err != nil {
		return step.PromptOutput{}, fmt.Errorf("llm propose source fix: %w", err)
	}
	nodeID := f.Target.NodeID
	base := fmt.Sprintf("proposed-fix/%s/%s/attempt-%d.source", f.Trigger.ReleaseID, nodeID, f.Attempt)
	draft.Edits = []remediation.Edit{{
		Path:         path.Join(repoPrefix, bundle.FilePath),
		Original:     bundle.Source,
		Content:      out.ProposedSQL,
		TargetNodeID: nodeID,
		ContentKey:   base + ".sql",
		DiffKey:      base + ".diff",
	}}
	draft.SourceResolved = true
	draft.Rationale = out.Rationale
	draft.Confidence = out.Confidence
	return step.PromptOutput{Draft: draft}, nil
}
