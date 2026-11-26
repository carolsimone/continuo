package shared

import (
	"context"
	"fmt"
	"log/slog"
	"slices"

	"github.com/carolsimone/continuo/agent-remediation/domain/proposal"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/domain/repository"
	"github.com/carolsimone/continuo/agent-remediation/serialization"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/agent-remediation/service/runner/step"
)

// PreviousTries is the context source `previous_tries`: every earlier try at
// the cluster's failure in its release, in this remediation round and the ones
// before it, each with what it changed and why it was refused. It reads the
// runner's own tries from their journal and the attempts recorded with no
// remediation from their proposal rows.
type PreviousTries struct {
	tries    repository.TryLister
	attempts repository.AttemptLister
	evidence ports.EvidenceReader
	logger   *slog.Logger
}

var _ step.ContextSource = (*PreviousTries)(nil)

// NewPreviousTries builds the previous_tries step. A nil tries reader gives no
// runner tries, a nil attempts reader no attempts with no remediation, and a nil
// evidence reader such attempts with no diffs. It calls no port until Gather.
func NewPreviousTries(tries repository.TryLister, attempts repository.AttemptLister, evidence ports.EvidenceReader,
	logger *slog.Logger) *PreviousTries {
	return &PreviousTries{tries: tries, attempts: attempts, evidence: evidence, logger: LoggerOrDefault(logger)}
}

// Meta describes the step.
func (*PreviousTries) Meta() step.Meta {
	return step.Meta{
		Name: "previous_tries",
		Kind: step.KindContext,
		Description: "Lists the earlier tries at the cluster's failing nodes in this release, in this round and the rounds before it, oldest first: " +
			"each try's diffs, what its pre-flight and cross-cluster checks found and why its release check failed. " +
			"Attempts recorded with no remediation are listed with their verify error and diffs, " +
			"except one the startup sweep failed, which no release check judged.",
		Needs:    []remediation.EvidenceKey{remediation.KeyFailure},
		Provides: []remediation.EvidenceKey{remediation.KeyPreviousTries},
	}
}

// Gather returns the tries made before the failure's own (its trigger's round
// and its try) whose clusters held any of its member nodes, oldest first. The
// runner's tries come from the journal of every remediation of the release; a
// try whose journal does not decode is left out and logged. The attempts with
// no remediation are the release's proposal rows of the trigger's source that
// belong to no remediation and name a member, except a row the startup sweep
// failed (nothing judged it); a diff of theirs that cannot be read is left out
// and logged, since the attempt still shows with its error. An error reading
// the journal or the proposal rows is the step's, so the runner tries the step
// again rather than show the model a partial history.
func (s *PreviousTries) Gather(ctx context.Context, in step.ContextInput) (step.ContextOutput, error) {
	f := in.Failure
	records, err := s.runnerTries(ctx, f)
	if err != nil {
		return step.ContextOutput{}, err
	}
	unowned, err := s.attemptsWithNoRemediation(ctx, f)
	if err != nil {
		return step.ContextOutput{}, err
	}
	earlier := remediation.TryRecords(append(records, unowned...), memberIDs(f), remediation.TryRef{Round: f.Trigger.Round, Try: f.Try})
	return step.ContextOutput{Evidence: map[remediation.EvidenceKey]any{remediation.KeyPreviousTries: earlier}}, nil
}

// runnerTries reads every try of the failure's release from the journal.
func (s *PreviousTries) runnerTries(ctx context.Context, f remediation.Failure) ([]remediation.TryRecord, error) {
	if s.tries == nil {
		return nil, nil
	}
	snaps, err := s.tries.ListTriesOfRelease(ctx, f.Trigger.TenantID, f.Trigger.ReleaseID)
	if err != nil {
		return nil, fmt.Errorf("list the tries of release %s: %w", f.Trigger.ReleaseID, err)
	}
	out := make([]remediation.TryRecord, 0, len(snaps))
	for _, snap := range snaps {
		rec, err := serialization.TryRecordOf(snap)
		if err != nil {
			s.logger.Warn("an earlier try's journal cannot be read; leaving it out",
				"remediation", snap.Remediation.ID, "round", snap.Remediation.Round, "error", err)
			continue
		}
		out = append(out, rec)
	}
	return out, nil
}

// attemptsWithNoRemediation reads the attempts at the failure's members that
// belong to no remediation from their proposal rows, each as a record of one
// cluster holding the nodes the attempt addressed, its diffs and its
// verify error.
func (s *PreviousTries) attemptsWithNoRemediation(ctx context.Context, f remediation.Failure) ([]remediation.TryRecord, error) {
	if s.attempts == nil {
		return nil, nil
	}
	views, err := s.attempts.List(ctx, repository.ProposalFilter{ReleaseID: f.Trigger.ReleaseID, Source: f.Trigger.Source})
	if err != nil {
		return nil, fmt.Errorf("list the proposals of release %s: %w", f.Trigger.ReleaseID, err)
	}
	members := memberIDs(f)
	var out []remediation.TryRecord
	for _, v := range views {
		if v.RemediationID != "" || sweptAtStartup(v) || !namesAny(v.ResolvedNodeIDs, members) {
			continue
		}
		rec := remediation.TryRecord{
			Round:    v.RemediationRound,
			Attempt:  v.Attempt,
			Clusters: []remediation.ClusterRecord{{Members: append([]string(nil), v.ResolvedNodeIDs...), Edits: s.attemptDiffs(ctx, v)}},
		}
		if v.VerifyError != "" {
			rec.Gate = []remediation.Finding{{Message: v.VerifyError}}
		}
		out = append(out, rec)
	}
	return out, nil
}

// sweptAtStartup reports whether the startup sweep failed an attempt row with no
// remediation: the sweep stores its reason as the verify error of a verifying
// row and as the rationale of a generating one. No gate judged such a row, so it
// is no evidence that its change fails.
func sweptAtStartup(v proposal.View) bool {
	return v.VerifyError == proposal.NoRemediationInFlightReason || v.Rationale == proposal.NoRemediationInFlightReason
}

// attemptDiffs reads an attempt's stored diffs. An edit with no diff, or whose
// diff cannot be read, is left out, and a nil evidence reader reads none.
func (s *PreviousTries) attemptDiffs(ctx context.Context, v proposal.View) []remediation.EditRecord {
	if s.evidence == nil {
		return nil
	}
	var out []remediation.EditRecord
	for _, e := range v.Edits {
		if e.DiffURI == "" {
			continue
		}
		raw, err := s.evidence.Fetch(ctx, e.DiffURI)
		if err != nil {
			s.logger.Warn("an earlier attempt's diff cannot be read; leaving it out",
				"attempt", v.Attempt, "diff_uri", e.DiffURI, "error", err)
			continue
		}
		out = append(out, remediation.EditRecord{Path: e.Path, Diff: raw})
	}
	return out
}

// memberIDs is the failure's member node ids, or its target's when it names no
// member.
func memberIDs(f remediation.Failure) []string {
	ids := make([]string, 0, len(f.Members))
	for _, m := range f.Members {
		ids = append(ids, m.NodeID)
	}
	if len(ids) == 0 {
		ids = append(ids, f.Target.NodeID)
	}
	return ids
}

func namesAny(nodeIDs, members []string) bool {
	for _, id := range members {
		if slices.Contains(nodeIDs, id) {
			return true
		}
	}
	return false
}
