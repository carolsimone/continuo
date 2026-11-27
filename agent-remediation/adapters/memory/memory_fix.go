package memory

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/carolsimone/continuo/agent-remediation/domain/fix"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/domain/repository"
)

// fixRepository is the in-memory repository.FixRepository. It covers
// what the runner calls; the pull-request methods return
// errors.ErrUnsupported.
type fixRepository struct {
	gate gate
}

var _ repository.FixRepository = (*fixRepository)(nil)

func roundOrDefault(n int) int {
	if n < 1 {
		return 1
	}
	return n
}

func (d *data) findFix(releaseID string, attempt int) *fixRow {
	for _, row := range d.fixes {
		if row.p.ReleaseID == releaseID && row.p.Attempt == attempt {
			return row
		}
	}
	return nil
}

func (d *data) findFixByID(id string) *fixRow {
	for _, row := range d.fixes {
		if row.id == id {
			return row
		}
	}
	return nil
}

// checkRemediation is the fix.remediation_id foreign key.
func (d *data) checkRemediation(id string) error {
	if id == "" {
		return nil
	}
	parsed, err := uuid.Parse(id)
	if err != nil {
		return fmt.Errorf("fix remediation id %q: %w", id, err)
	}
	if _, ok := d.remediations[parsed]; !ok {
		return fmt.Errorf("fix remediation id %s: no such remediation", id)
	}
	return nil
}

// CountAttempts counts the terminal attempts of one round of a release.
func (r *fixRepository) CountAttempts(_ context.Context, releaseID string, remediationRound int) (int, error) {
	var n int
	err := r.gate(func(d *data) error {
		for _, row := range d.fixes {
			p := row.p
			if p.ReleaseID == releaseID && p.RemediationRound == remediationRound &&
				p.Status != fix.StatusGenerating && p.Status != fix.StatusVerifying {
				n++
			}
		}
		return nil
	})
	return n, err
}

// InsertGenerating inserts the in-flight row of an attempt, and does nothing
// when the (release, attempt) row exists.
func (r *fixRepository) InsertGenerating(_ context.Context, p fix.Fix) error {
	return r.gate(func(d *data) error {
		if d.findFix(p.ReleaseID, p.Attempt) != nil {
			return nil
		}
		if err := d.checkRemediation(p.RemediationID); err != nil {
			return err
		}
		d.fixes = append(d.fixes, &fixRow{id: uuid.NewString(), p: cloneFix(fix.Fix{
			Source:           p.Source,
			ReleaseID:        p.ReleaseID,
			RemediationID:    p.RemediationID,
			RemediationRound: roundOrDefault(p.RemediationRound),
			NodeID:           p.NodeID,
			ResolvedNodeIDs:  p.ResolvedNodeIDs,
			ErrorSignature:   p.ErrorSignature,
			Attempt:          p.Attempt,
			Status:           fix.StatusGenerating,
			NodeOutcomes:     p.NodeOutcomes,
			Projects:         p.Projects,
			CreatedAt:        p.CreatedAt,
		})})
		return nil
	})
}

// FailGenerating fails the in-flight rows of a release and the per-node
// outcomes still marked generating.
func (r *fixRepository) FailGenerating(_ context.Context, releaseID, reason string) (int, error) {
	var n int
	err := r.gate(func(d *data) error {
		for _, row := range d.fixes {
			if row.p.ReleaseID != releaseID || row.p.Status != fix.StatusGenerating {
				continue
			}
			row.p.Status = fix.StatusFailed
			row.p.Rationale = reason
			for node, o := range row.p.NodeOutcomes {
				if o.Status == fix.StatusGenerating {
					row.p.NodeOutcomes[node] = fix.NodeOutcome{Status: fix.StatusFailed, Reason: reason}
				}
			}
			n++
		}
		return nil
	})
	return n, err
}

// FailLegacyInFlight fails every generating or verifying row that belongs to
// no remediation, as the Postgres repository does: a row that was generating
// takes the reason as its rationale, a row that was verifying takes it as its
// verify error, and each node outcome still generating or verifying becomes
// failed with the reason.
func (r *fixRepository) FailLegacyInFlight(_ context.Context, reason string) (int, error) {
	var n int
	err := r.gate(func(d *data) error {
		for _, row := range d.fixes {
			inFlight := row.p.Status == fix.StatusGenerating || row.p.Status == fix.StatusVerifying
			if row.p.RemediationID != "" || !inFlight {
				continue
			}
			if row.p.Status == fix.StatusGenerating {
				row.p.Rationale = reason
			} else {
				row.verifyError = reason
			}
			row.p.Status = fix.StatusFailed
			for node, o := range row.p.NodeOutcomes {
				if o.Status == fix.StatusGenerating || o.Status == fix.StatusVerifying {
					row.p.NodeOutcomes[node] = fix.NodeOutcome{Status: fix.StatusFailed, Reason: reason}
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
func (r *fixRepository) Upsert(_ context.Context, p fix.Fix) error {
	return r.gate(func(d *data) error {
		if err := d.checkRemediation(p.RemediationID); err != nil {
			return err
		}
		row := d.findFix(p.ReleaseID, p.Attempt)
		if row == nil {
			p.RemediationRound = roundOrDefault(p.RemediationRound)
			d.fixes = append(d.fixes, &fixRow{id: uuid.NewString(), p: cloneFix(p)})
			return nil
		}
		old := row.p
		next := cloneFix(p)
		next.Source = old.Source
		next.RemediationRound = old.RemediationRound
		next.NodeID = old.NodeID
		next.ErrorSignature = old.ErrorSignature
		if next.RemediationID == "" {
			next.RemediationID = old.RemediationID
		}
		if len(next.Approvers) == 0 {
			next.Approvers = old.Approvers
		}
		row.p = next
		return nil
	})
}

// view projects a stored row as the read model. A row with no resolved nodes
// reads as the one node in NodeID, and a row with no edits but a file path
// reads as one edit of that file.
func (row *fixRow) view() fix.View {
	p := cloneFix(row.p)
	resolved := p.ResolvedNodeIDs
	if len(resolved) == 0 && p.NodeID != "" {
		resolved = []string{p.NodeID}
	}
	edits := p.Edits
	if len(edits) == 0 && p.FilePath != "" {
		edits = []fix.FileEdit{{Path: p.FilePath, ContentURI: p.ProposedSQLURI, DiffURI: p.DiffURI}}
	}
	if len(edits) == 0 {
		edits = nil
	}
	outcomes := p.NodeOutcomes
	if len(outcomes) == 0 {
		outcomes = nil
	}
	releaseChecks := p.ReleaseChecks
	if len(releaseChecks) == 0 {
		releaseChecks = nil
	}
	approvers := p.Approvers
	if len(approvers) == 0 {
		approvers = nil
	}
	return fix.View{
		ID:                  row.id,
		Source:              p.Source,
		ReleaseID:           p.ReleaseID,
		RemediationRound:    p.RemediationRound,
		RemediationID:       p.RemediationID,
		NodeID:              p.NodeID,
		ResolvedNodeIDs:     resolved,
		ErrorSignature:      p.ErrorSignature,
		Attempt:             p.Attempt,
		Status:              p.Status,
		NodeOutcomes:        outcomes,
		ReleaseChecks:       releaseChecks,
		Approvers:           approvers,
		ReleaseCheckID:      p.ReleaseCheckID,
		Projects:            p.Projects,
		VerifyError:         row.verifyError,
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

// Get returns one fix.
func (r *fixRepository) Get(_ context.Context, id string) (fix.View, error) {
	var v fix.View
	err := r.gate(func(d *data) error {
		row := d.findFixByID(id)
		if row == nil {
			return repository.ErrNotFound
		}
		v = row.view()
		return nil
	})
	return v, err
}

// List returns the fixes matching the filter, newest first. A pull-request
// state filter returns errors.ErrUnsupported: the store keeps no pull requests.
func (r *fixRepository) List(_ context.Context, filter repository.FixFilter) ([]fix.View, error) {
	if filter.PRState != "" {
		return nil, errors.ErrUnsupported
	}
	var out []fix.View
	err := r.gate(func(d *data) error {
		for _, row := range d.fixes {
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
			if filter.Project != "" && !containsString(v.Projects, filter.Project) {
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

// finishVerifying moves a verifying row to status, writing release checks and
// carrying every node outcome still verifying to the same status. It reports
// whether the compare-and-set on 'verifying' fired.
func (d *data) finishVerifying(id string, status fix.Status, verifyError string, releaseChecks []fix.ReleaseCheck) bool {
	row := d.findFixByID(id)
	if row == nil || row.p.Status != fix.StatusVerifying {
		return false
	}
	row.p.Status = status
	row.p.ReleaseChecks = cloneReleaseChecks(releaseChecks)
	if status == fix.StatusFailed {
		row.verifyError = verifyError
	}
	for node, o := range row.p.NodeOutcomes {
		if o.Status == fix.StatusVerifying {
			o.Status = status
			row.p.NodeOutcomes[node] = o
		}
	}
	return true
}

// MarkFixVerified refuses a zero Verified and otherwise moves the verifying
// fix it names to proposed, writing the release checks it carries.
func (r *fixRepository) MarkFixVerified(_ context.Context, v remediation.Verified) (bool, error) {
	if v.IsZero() {
		return false, errors.New("mark fix verified: the verified fix is the zero value")
	}
	var hit bool
	err := r.gate(func(d *data) error {
		hit = d.finishVerifying(v.FixID(), fix.StatusProposed, "", v.ReleaseChecks())
		return nil
	})
	return hit, err
}

// MarkVerifyFailed moves a verifying fix to failed and records the error.
func (r *fixRepository) MarkVerifyFailed(_ context.Context, id, verifyErr string, releaseChecks []fix.ReleaseCheck) (bool, error) {
	var hit bool
	err := r.gate(func(d *data) error {
		hit = d.finishVerifying(id, fix.StatusFailed, verifyErr, releaseChecks)
		return nil
	})
	return hit, err
}

// ReopenTryAttempt moves the failed row of the remediation's attempt back to
// status, clearing the failure the row records. Reopened generating, the
// rationale is cleared and each outcome FailGenerating failed is generating
// again (reopenedGenerating); reopened verifying, the outcomes are outcomes.
func (r *fixRepository) ReopenTryAttempt(_ context.Context, remediationID uuid.UUID, attempt int, status fix.Status, outcomes map[string]fix.NodeOutcome) (bool, error) {
	switch status {
	case fix.StatusGenerating:
		if outcomes != nil {
			return false, fmt.Errorf("reopen attempt %d: a row reopened generating takes back the outcomes it had, not outcomes given", attempt)
		}
	case fix.StatusVerifying:
	default:
		return false, fmt.Errorf("reopen attempt %d: status %q is not generating or verifying", attempt, status)
	}
	var reopened bool
	err := r.gate(func(d *data) error {
		for _, row := range d.fixes {
			if row.p.RemediationID != remediationID.String() || row.p.Attempt != attempt || row.p.Status != fix.StatusFailed {
				continue
			}
			if status == fix.StatusGenerating {
				row.p.NodeOutcomes = reopenedGenerating(row.p.NodeOutcomes, row.p.Rationale)
				row.p.Rationale = ""
			} else {
				row.p.NodeOutcomes = maps.Clone(outcomes)
			}
			row.p.Status = status
			row.verifyError = ""
			reopened = true
		}
		return nil
	})
	return reopened, err
}

// reopenedGenerating undoes FailGenerating's rewrite of outcomes, failed with
// reason: an outcome failed with reason and no code, which is how
// FailGenerating fails an outcome that was generating, is generating again,
// and every other outcome is kept.
func reopenedGenerating(outcomes map[string]fix.NodeOutcome, reason string) map[string]fix.NodeOutcome {
	if outcomes == nil {
		return nil
	}
	out := make(map[string]fix.NodeOutcome, len(outcomes))
	for node, o := range outcomes {
		if o.Status == fix.StatusFailed && o.Code == "" && o.Reason == reason {
			o = fix.NodeOutcome{Status: fix.StatusGenerating}
		}
		out[node] = o
	}
	return out
}

// UpdateReleaseCheckPhase sets the phase and activation time of the
// release check with checkID, leaving the others alone.
func (r *fixRepository) UpdateReleaseCheckPhase(_ context.Context, id, checkID string, phase fix.Phase, activatedAt *time.Time) error {
	return r.gate(func(d *data) error {
		row := d.findFixByID(id)
		if row == nil {
			return nil
		}
		for i := range row.p.ReleaseChecks {
			if row.p.ReleaseChecks[i].CheckID == checkID {
				row.p.ReleaseChecks[i].Phase = phase
				row.p.ReleaseChecks[i].ActivatedAt = cloneTimePtr(activatedAt)
			}
		}
		return nil
	})
}

// RecordApprovers adds approvers to the row of the attempt, as
// fix.MergeApprovers does. It is ErrNotFound when the release has no row
// for the attempt.
func (r *fixRepository) RecordApprovers(_ context.Context, releaseID string, attempt int, approvers []fix.Approver) error {
	return r.gate(func(d *data) error {
		row := d.findFix(releaseID, attempt)
		if row == nil {
			return repository.ErrNotFound
		}
		row.p.Approvers = fix.MergeApprovers(row.p.Approvers, approvers)
		return nil
	})
}

func (r *fixRepository) BeginPR(context.Context, string, string, string, time.Time) (fix.PRClaim, error) {
	return fix.PRClaim{}, errors.ErrUnsupported
}

func (r *fixRepository) RecordPR(context.Context, string, string, string, int, string, time.Time) (bool, error) {
	return false, errors.ErrUnsupported
}

func (r *fixRepository) FailStuckOpeningPR(context.Context, string, string, time.Time) (bool, error) {
	return false, errors.ErrUnsupported
}

func (r *fixRepository) ListOpenPullRequests(context.Context, int) ([]fix.OpenPR, error) {
	return nil, errors.ErrUnsupported
}

func (r *fixRepository) ListStuckOpening(context.Context, int, *repository.OpeningCursor) ([]fix.OpeningPR, *repository.OpeningCursor, error) {
	return nil, nil, errors.ErrUnsupported
}

func (r *fixRepository) RecordPROutcome(context.Context, string, string, fix.PROutcome, time.Time) (bool, error) {
	return false, errors.ErrUnsupported
}
