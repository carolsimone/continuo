package memory

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/carolsimone/continuo/agent-remediation/domain/proposal"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/domain/repository"
)

// verifyingBatchLimit caps ListVerifying as the Postgres repository does.
const verifyingBatchLimit = 20

// proposalRepository is the in-memory repository.ProposalRepository. It covers
// what the proposal driver, the verification reconciler and the runner call;
// the pull-request methods return errors.ErrUnsupported.
type proposalRepository struct {
	gate gate
}

var _ repository.ProposalRepository = (*proposalRepository)(nil)

func roundOrDefault(n int) int {
	if n < 1 {
		return 1
	}
	return n
}

func (d *data) findProposal(releaseID string, attempt int) *proposalRow {
	for _, row := range d.proposals {
		if row.p.ReleaseID == releaseID && row.p.Attempt == attempt {
			return row
		}
	}
	return nil
}

func (d *data) findProposalByID(id string) *proposalRow {
	for _, row := range d.proposals {
		if row.id == id {
			return row
		}
	}
	return nil
}

// checkRemediation is the proposal.remediation_id foreign key.
func (d *data) checkRemediation(id string) error {
	if id == "" {
		return nil
	}
	parsed, err := uuid.Parse(id)
	if err != nil {
		return fmt.Errorf("proposal remediation id %q: %w", id, err)
	}
	if _, ok := d.remediations[parsed]; !ok {
		return fmt.Errorf("proposal remediation id %s: no such remediation", id)
	}
	return nil
}

// CountAttempts counts the terminal attempts of one round of a release.
func (r *proposalRepository) CountAttempts(_ context.Context, releaseID string, remediationRound int) (int, error) {
	var n int
	err := r.gate(func(d *data) error {
		for _, row := range d.proposals {
			p := row.p
			if p.ReleaseID == releaseID && p.RemediationRound == remediationRound &&
				p.Status != proposal.StatusGenerating && p.Status != proposal.StatusVerifying {
				n++
			}
		}
		return nil
	})
	return n, err
}

// InsertGenerating inserts the in-flight row of an attempt, and does nothing
// when the (release, attempt) row exists.
func (r *proposalRepository) InsertGenerating(_ context.Context, p proposal.Proposal) error {
	return r.gate(func(d *data) error {
		if d.findProposal(p.ReleaseID, p.Attempt) != nil {
			return nil
		}
		if err := d.checkRemediation(p.RemediationID); err != nil {
			return err
		}
		d.proposals = append(d.proposals, &proposalRow{id: uuid.NewString(), p: cloneProposal(proposal.Proposal{
			Source:           p.Source,
			ReleaseID:        p.ReleaseID,
			RemediationID:    p.RemediationID,
			RemediationRound: roundOrDefault(p.RemediationRound),
			NodeID:           p.NodeID,
			ResolvedNodeIDs:  p.ResolvedNodeIDs,
			ErrorSignature:   p.ErrorSignature,
			Attempt:          p.Attempt,
			Status:           proposal.StatusGenerating,
			NodeOutcomes:     p.NodeOutcomes,
			Services:         p.Services,
			CreatedAt:        p.CreatedAt,
		})})
		return nil
	})
}

// FailGenerating fails the in-flight rows of a release and the per-node
// outcomes still marked generating.
func (r *proposalRepository) FailGenerating(_ context.Context, releaseID, reason string) (int, error) {
	var n int
	err := r.gate(func(d *data) error {
		for _, row := range d.proposals {
			if row.p.ReleaseID != releaseID || row.p.Status != proposal.StatusGenerating {
				continue
			}
			row.p.Status = proposal.StatusFailed
			row.p.Rationale = reason
			for node, o := range row.p.NodeOutcomes {
				if o.Status == proposal.StatusGenerating {
					row.p.NodeOutcomes[node] = proposal.NodeOutcome{Status: proposal.StatusFailed, Reason: reason}
				}
			}
			n++
		}
		return nil
	})
	return n, err
}

// Upsert inserts the attempt's row or finalizes the row it conflicts with on
// (release, attempt). The conflict update leaves the identity columns (source,
// round, node, signature) as the first write set them, and leaves the
// remediation as it was when p names none.
func (r *proposalRepository) Upsert(_ context.Context, p proposal.Proposal) error {
	return r.gate(func(d *data) error {
		if err := d.checkRemediation(p.RemediationID); err != nil {
			return err
		}
		row := d.findProposal(p.ReleaseID, p.Attempt)
		if row == nil {
			p.RemediationRound = roundOrDefault(p.RemediationRound)
			if len(p.TriggerPayload) == 0 {
				p.TriggerPayload = []byte("{}")
			}
			d.proposals = append(d.proposals, &proposalRow{id: uuid.NewString(), p: cloneProposal(p)})
			return nil
		}
		old := row.p
		next := cloneProposal(p)
		next.Source = old.Source
		next.RemediationRound = old.RemediationRound
		next.NodeID = old.NodeID
		next.ErrorSignature = old.ErrorSignature
		if len(next.TriggerPayload) == 0 {
			next.TriggerPayload = []byte("{}")
		}
		if next.RemediationID == "" {
			next.RemediationID = old.RemediationID
		}
		row.p = next
		return nil
	})
}

// view projects a stored row as the read model. A row with no resolved nodes
// reads as the one node in NodeID, and a row with no edits but a file path
// reads as one edit of that file.
func (row *proposalRow) view() proposal.View {
	p := cloneProposal(row.p)
	resolved := p.ResolvedNodeIDs
	if len(resolved) == 0 && p.NodeID != "" {
		resolved = []string{p.NodeID}
	}
	edits := p.Edits
	if len(edits) == 0 && p.FilePath != "" {
		edits = []proposal.FileEdit{{Path: p.FilePath, ContentURI: p.ProposedSQLURI, DiffURI: p.DiffURI}}
	}
	if len(edits) == 0 {
		edits = nil
	}
	outcomes := p.NodeOutcomes
	if len(outcomes) == 0 {
		outcomes = nil
	}
	verifications := p.Verifications
	if len(verifications) == 0 {
		verifications = nil
	}
	return proposal.View{
		ID:                  row.id,
		Source:              p.Source,
		ReleaseID:           p.ReleaseID,
		RemediationRound:    p.RemediationRound,
		NodeID:              p.NodeID,
		ResolvedNodeIDs:     resolved,
		ErrorSignature:      p.ErrorSignature,
		Attempt:             p.Attempt,
		Status:              p.Status,
		NodeOutcomes:        outcomes,
		Verifications:       verifications,
		VerificationRunID:   p.VerificationRunID,
		Services:            p.Services,
		VerifyError:         row.verifyError,
		TriggerPayload:      p.TriggerPayload,
		Confidence:          p.Confidence,
		Rationale:           p.Rationale,
		ProposedSQLURI:      p.ProposedSQLURI,
		DiffURI:             p.DiffURI,
		CandidateFixSQLURI:  p.CandidateFixSQLURI,
		CandidateFixDiffURI: p.CandidateFixDiffURI,
		SourceResolved:      p.SourceResolved,
		Repo:                p.Repo,
		CommitSHA:           p.CommitSHA,
		FilePath:            p.FilePath,
		Edits:               edits,
		Model:               p.Model,
		CreatedAt:           p.CreatedAt,
	}
}

// Get returns one proposal.
func (r *proposalRepository) Get(_ context.Context, id string) (proposal.View, error) {
	var v proposal.View
	err := r.gate(func(d *data) error {
		row := d.findProposalByID(id)
		if row == nil {
			return repository.ErrNotFound
		}
		v = row.view()
		return nil
	})
	return v, err
}

// List returns the proposals matching the filter, newest first. A pull-request
// state filter returns errors.ErrUnsupported: the store keeps no pull requests.
func (r *proposalRepository) List(_ context.Context, filter repository.ProposalFilter) ([]proposal.View, error) {
	if filter.PRState != "" {
		return nil, errors.ErrUnsupported
	}
	var out []proposal.View
	err := r.gate(func(d *data) error {
		for _, row := range d.proposals {
			v := row.view()
			if filter.Status != "" && string(v.Status) != filter.Status {
				continue
			}
			if filter.ReleaseID != "" && v.ReleaseID != filter.ReleaseID {
				continue
			}
			if filter.Source != "" && v.Source != filter.Source {
				continue
			}
			if filter.NodeID != "" && !containsString(row.p.ResolvedNodeIDs, filter.NodeID) {
				continue
			}
			if filter.Service != "" && !containsString(v.Services, filter.Service) {
				continue
			}
			out = append(out, v)
		}
		sort.SliceStable(out, func(i, j int) bool { return out[i].CreatedAt.After(out[j].CreatedAt) })
		if filter.Limit > 0 && len(out) > filter.Limit {
			out = out[:filter.Limit]
		}
		return nil
	})
	return out, err
}

func containsString(list []string, s string) bool {
	for _, e := range list {
		if e == s {
			return true
		}
	}
	return false
}

// ListVerifying returns the driver's verifying proposals, oldest first, capped
// at the batch limit. A proposal of a remediation is left out.
func (r *proposalRepository) ListVerifying(context.Context) ([]proposal.View, error) {
	var out []proposal.View
	err := r.gate(func(d *data) error {
		for _, row := range d.proposals {
			if row.p.Status == proposal.StatusVerifying && row.p.RemediationID == "" {
				out = append(out, row.view())
			}
		}
		sort.SliceStable(out, func(i, j int) bool { return out[i].CreatedAt.Before(out[j].CreatedAt) })
		if len(out) > verifyingBatchLimit {
			out = out[:verifyingBatchLimit]
		}
		return nil
	})
	return out, err
}

// finishVerifying moves a verifying row to status, writing verifications and
// carrying every node outcome still verifying to the same status. It reports
// whether the compare-and-set on 'verifying' fired.
func (d *data) finishVerifying(id string, status proposal.Status, verifyError string, verifications []proposal.Verification) bool {
	row := d.findProposalByID(id)
	if row == nil || row.p.Status != proposal.StatusVerifying {
		return false
	}
	row.p.Status = status
	row.p.Verifications = cloneVerifications(verifications)
	if status == proposal.StatusFailed {
		row.verifyError = verifyError
	}
	for node, o := range row.p.NodeOutcomes {
		if o.Status == proposal.StatusVerifying {
			o.Status = status
			row.p.NodeOutcomes[node] = o
		}
	}
	return true
}

// MarkVerified moves a verifying proposal to proposed.
func (r *proposalRepository) MarkVerified(_ context.Context, id string, verifications []proposal.Verification) (bool, error) {
	var hit bool
	err := r.gate(func(d *data) error {
		hit = d.finishVerifying(id, proposal.StatusProposed, "", verifications)
		return nil
	})
	return hit, err
}

// MarkFixVerified refuses a zero Verified and otherwise does what MarkVerified
// does with the proposal id and verifications it carries.
func (r *proposalRepository) MarkFixVerified(ctx context.Context, v remediation.Verified) (bool, error) {
	if v.IsZero() {
		return false, errors.New("mark fix verified: the verified proposal is the zero value")
	}
	return r.MarkVerified(ctx, v.ProposalID(), v.Verifications())
}

// MarkVerifyFailed moves a verifying proposal to failed and records the error.
func (r *proposalRepository) MarkVerifyFailed(_ context.Context, id, verifyErr string, verifications []proposal.Verification) (bool, error) {
	var hit bool
	err := r.gate(func(d *data) error {
		hit = d.finishVerifying(id, proposal.StatusFailed, verifyErr, verifications)
		return nil
	})
	return hit, err
}

// UpdateVerificationPhase sets the phase and activation time of the
// verification with runID, leaving the others alone.
func (r *proposalRepository) UpdateVerificationPhase(_ context.Context, id, runID string, phase proposal.Phase, activatedAt *time.Time) error {
	return r.gate(func(d *data) error {
		row := d.findProposalByID(id)
		if row == nil {
			return nil
		}
		for i := range row.p.Verifications {
			if row.p.Verifications[i].RunID == runID {
				row.p.Verifications[i].Phase = phase
				row.p.Verifications[i].ActivatedAt = cloneTimePtr(activatedAt)
			}
		}
		return nil
	})
}

func (r *proposalRepository) BeginPR(context.Context, string, string, string, time.Time) (proposal.PRClaim, error) {
	return proposal.PRClaim{}, errors.ErrUnsupported
}

func (r *proposalRepository) RecordPR(context.Context, string, string, string, int, string, time.Time) (bool, error) {
	return false, errors.ErrUnsupported
}

func (r *proposalRepository) FailStuckOpeningPR(context.Context, string, string, time.Time) (bool, error) {
	return false, errors.ErrUnsupported
}

func (r *proposalRepository) ListOpenPullRequests(context.Context, int) ([]proposal.OpenPR, error) {
	return nil, errors.ErrUnsupported
}

func (r *proposalRepository) ListStuckOpening(context.Context, int, *repository.OpeningCursor) ([]proposal.OpeningPR, *repository.OpeningCursor, error) {
	return nil, nil, errors.ErrUnsupported
}

func (r *proposalRepository) RecordPROutcome(context.Context, string, string, proposal.PROutcome, time.Time) (bool, error) {
	return false, errors.ErrUnsupported
}
