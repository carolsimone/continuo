package fixer

import (
	"context"
	"errors"
	"fmt"
	"path"

	"github.com/carolsimone/continuo/agent-remediation/domain/prompt"
	"github.com/carolsimone/continuo/agent-remediation/domain/proposal"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
)

// MemberFailure is one failing descendant of a shared-upstream cluster.
type MemberFailure struct {
	NodeID         string
	ErrorSignature string
	Category       string
	Reason         string
	ErrorExcerpt   string
}

// UpstreamInput is the evidence for a shared-upstream cluster: the changed
// ancestor to edit and the failing descendants that share its change as
// their cause.
type UpstreamInput struct {
	ReleaseID     string
	Repo          string
	CommitSHA     string
	CodeBundleURI string
	TargetNodeID  string
	// TargetFilePath and TargetService are where the rejected release's
	// candidate topology declares the ancestor, carried on the trigger. Both
	// empty when the rejection carried no location for it, and the promoted
	// graph is consulted instead.
	TargetFilePath string
	TargetService  string
	Attempt        int
	Members        []MemberFailure
}

// ProposeUpstreamFix repairs the changed ancestor of a shared-upstream cluster
// with one model call and returns its corrected source as a single edit whose
// TargetNodeID is the ancestor. The ancestor's source comes from the release's
// code bundle (it changed in this release, so the bundle holds it whether or
// not it failed); its location comes from the promoted graph, so a brand-new
// ancestor — which the promoted graph cannot place — ends the cluster as
// skipped and the driver falls back to fixing the members independently. A
// non-dbt ancestor is skipped the same way.
func ProposeUpstreamFix(ctx context.Context, svc Services, in UpstreamInput) (Result, error) {
	if len(in.Members) == 0 {
		return skipUpstream(svc, in, "an upstream cluster needs at least one failing member"), nil
	}
	src, err := svc.CandidateSource.NodeSource(ctx, in.CodeBundleURI, in.TargetNodeID, in.ReleaseID)
	if errors.Is(err, ports.ErrNotFound) {
		return skipUpstream(svc, in, "the changed ancestor's source is not in the release's code bundle"), nil
	}
	if err != nil {
		return Result{}, fmt.Errorf("fetch code bundle for %s: %w", in.TargetNodeID, err)
	}
	if src.Runtime != ports.RuntimeDbt {
		return skipUpstream(svc, in, fmt.Sprintf("the changed ancestor %s is not a dbt model (runtime %q)", in.TargetNodeID, src.Runtime)), nil
	}
	filePath, serviceName, err := locateUpstreamTarget(ctx, svc, in)
	if err != nil || filePath == "" || serviceName == "" {
		return skipUpstream(svc, in, fmt.Sprintf("the changed ancestor %s cannot be located in the promoted graph", in.TargetNodeID)), nil
	}
	prefix, ok := svc.ServiceRepoPaths[serviceName]
	if !ok {
		return skipUpstream(svc, in, fmt.Sprintf("service %q has no repository path mapping", serviceName)), nil
	}
	fullPath := path.Join(prefix, filePath)

	ownChangeDiff := ""
	if cur, ok, verr := svc.Versions.CurrentVersion(ctx, in.TargetNodeID); verr != nil {
		svc.Logger.Warn("current version unavailable; omitting own-change diff", "node", in.TargetNodeID, "error", verr)
	} else if ok {
		ownChangeDiff = truncateDiff(svc.Sanitizer.Sanitize(proposal.ComputeUnifiedDiff(cur.RawCode, src.RawCode, in.TargetNodeID)), maxUpstreamDiffBytes)
	}

	members := make([]prompt.MemberFailure, 0, len(in.Members))
	for _, m := range in.Members {
		members = append(members, prompt.MemberFailure{NodeID: m.NodeID, ErrorExcerpt: svc.Sanitizer.Sanitize(m.ErrorExcerpt)})
	}
	first := in.Members[0]
	precedents := loadPrecedents(ctx, svc, Input{ReleaseID: in.ReleaseID, NodeID: first.NodeID,
		ErrorSignature: first.ErrorSignature, Category: first.Category, Reason: first.Reason})

	res, err := svc.LLM.Propose(ctx, prompt.AssembleUpstreamFix(prompt.UpstreamEvidence{
		TargetNodeID: in.TargetNodeID, TargetSource: svc.Sanitizer.Sanitize(src.RawCode),
		OwnChangeDiff: ownChangeDiff, Members: members, Precedents: precedents,
	}))
	if err != nil {
		return Result{}, fmt.Errorf("llm propose: %w", err)
	}
	// An answer the ancestor cannot be repaired from ends the cluster skipped,
	// not failed: each member can still be fixed in its own source, and the
	// driver only falls back to that when the upstream attempt skips. Failing
	// here would abandon every member on one declined answer.
	if why := declined(res, src.RawCode); why != "" {
		return skipUpstream(svc, in, fmt.Sprintf(
			"the model could not produce a safe fix for the changed ancestor %s: %s", in.TargetNodeID, why)), nil
	}
	edit, err := writeSourceArtifacts(ctx, svc, Input{ReleaseID: in.ReleaseID, NodeID: in.TargetNodeID, Attempt: in.Attempt},
		fullPath, src.RawCode, res.ProposedSQL)
	if err != nil {
		return Result{}, err
	}
	edit.TargetNodeID = in.TargetNodeID
	return Result{Proposal: proposal.Proposal{
		Status: proposal.StatusProposed, Confidence: normalizeConfidence(res.Confidence), Rationale: res.Rationale,
		ProposedSQLURI: edit.ContentURI, DiffURI: edit.DiffURI, SourceResolved: true, Model: res.Model,
		Repo: in.Repo, CommitSHA: in.CommitSHA, FilePath: fullPath, Edits: []proposal.FileEdit{edit},
	}}, nil
}

// locateUpstreamTarget resolves where to edit the changed ancestor. The
// rejected release's own candidate location wins whenever the trigger carries
// it: this release may have renamed or moved the ancestor, in which case the
// promoted graph still points at the file it used to live in and an edit routed
// there would rewrite source that no longer declares the node. The promoted
// graph is the fallback for a rejection that carries no per-node topology.
func locateUpstreamTarget(ctx context.Context, svc Services, in UpstreamInput) (filePath, serviceName string, err error) {
	if in.TargetFilePath != "" && in.TargetService != "" {
		return in.TargetFilePath, in.TargetService, nil
	}
	return svc.Locator.Locate(ctx, in.TargetNodeID)
}

// declined names why the model's answer cannot repair the changed ancestor, or
// "" when it can: an empty answer, the ancestor's own source returned
// unchanged, and an answer the model itself rates low are all the model
// declining to fix the shared cause.
func declined(res ports.ProposeResult, original string) string {
	switch {
	case res.ProposedSQL == "":
		return "it returned no source"
	case res.ProposedSQL == original:
		return "it returned the ancestor's source unchanged"
	case isLowConfidence(res.Confidence):
		return "it reported low confidence in its answer"
	}
	return ""
}

func skipUpstream(svc Services, in UpstreamInput, reason string) Result {
	svc.Logger.Info("upstream fix skipped; members fall back to independent fixes", "target", in.TargetNodeID, "reason", reason)
	return Result{Proposal: proposal.Proposal{Status: proposal.StatusSkipped, Rationale: reason}}
}
