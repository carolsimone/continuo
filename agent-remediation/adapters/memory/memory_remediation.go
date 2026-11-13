package memory

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/domain/repository"
	"github.com/carolsimone/continuo/agent-remediation/domain/workflow"
)

// remediationRepository is the in-memory repository.RemediationRepository. It
// keeps the Postgres tables' rules: the natural key of a remediation, the
// status checks, the foreign keys from try to remediation and from cluster and
// step to try, the cascade of a delete, and the lease rules.
type remediationRepository struct {
	gate gate
}

var _ repository.RemediationRepository = (*remediationRepository)(nil)

// orNow is the column default of a timestamp: the given time, or now when zero.
func orNow(t time.Time) time.Time {
	if t.IsZero() {
		return time.Now()
	}
	return t
}

func leaseFree(r *remediation.Remediation, now time.Time) bool {
	return r.LeaseUntil == nil || r.LeaseUntil.Before(now)
}

func validStatus[T comparable](kind string, status T, all []T) error {
	if slices.Contains(all, status) {
		return nil
	}
	return fmt.Errorf("%s status %v violates its check constraint", kind, status)
}

// currentTry returns the try a remediation is on, or nil.
func (d *data) currentTry(r *remediation.Remediation) *remediation.Try {
	if r.CurrentTry == 0 {
		return nil
	}
	return d.tries[tryKey{r.ID, r.CurrentTry}]
}

// stepKinds is the set the remediation_step.kind check allows.
var stepKinds = []string{"context", "prompt", "check", "approver", "output", "runner"}

func (r *remediationRepository) Create(_ context.Context, rem remediation.Remediation) (bool, error) {
	var created bool
	err := r.gate(func(d *data) error {
		if err := validStatus("remediation", rem.Status, remediation.AllRemediationStatuses()); err != nil {
			return err
		}
		if rem.Round < 1 {
			return fmt.Errorf("remediation round %d violates its check constraint", rem.Round)
		}
		if rem.ID == uuid.Nil {
			return errors.New("create remediation: the id is unset")
		}
		if _, ok := d.remediations[rem.ID]; ok {
			return nil
		}
		for _, other := range d.remediations {
			if other.TenantID == rem.TenantID && other.ReleaseID == rem.ReleaseID && other.Round == rem.Round {
				return nil
			}
		}
		stored := cloneRemediation(rem)
		if len(stored.TriggerPayload) == 0 {
			stored.TriggerPayload = []byte("{}")
		}
		if len(stored.TriggerFields) == 0 {
			stored.TriggerFields = nil
		}
		stored.CreatedAt = orNow(stored.CreatedAt)
		stored.UpdatedAt = orNow(stored.UpdatedAt)
		d.remediations[stored.ID] = &stored
		created = true
		return nil
	})
	return created, err
}

func (r *remediationRepository) FindByRelease(_ context.Context, tenantID, releaseID string, round int) (remediation.Remediation, bool, error) {
	var (
		out   remediation.Remediation
		found bool
	)
	err := r.gate(func(d *data) error {
		for _, rem := range d.remediations {
			if rem.TenantID == tenantID && rem.ReleaseID == releaseID && rem.Round == round {
				out, found = cloneRemediation(*rem), true
			}
		}
		return nil
	})
	return out, found, err
}

// snapshot loads one remediation with its current try, clusters and steps. It
// errors when current_try names a try that does not exist.
func (d *data) snapshot(rem *remediation.Remediation) (remediation.Snapshot, error) {
	snap := remediation.Snapshot{Remediation: cloneRemediation(*rem)}
	if rem.CurrentTry == 0 {
		return snap, nil
	}
	try := d.currentTry(rem)
	if try == nil {
		return remediation.Snapshot{}, fmt.Errorf("load remediation %s: current try %d has no row", rem.ID, rem.CurrentTry)
	}
	t := cloneTry(*try)
	snap.Try = &t
	for key, c := range d.clusters {
		if key.remediationID == rem.ID && key.try == rem.CurrentTry {
			snap.Clusters = append(snap.Clusters, cloneCluster(*c))
		}
	}
	sort.Slice(snap.Clusters, func(i, j int) bool { return snap.Clusters[i].Cluster < snap.Clusters[j].Cluster })
	for _, s := range d.steps {
		if s.RemediationID == rem.ID && s.Try == rem.CurrentTry {
			snap.Steps = append(snap.Steps, cloneStep(*s))
		}
	}
	return snap, nil
}

func (r *remediationRepository) Load(_ context.Context, id uuid.UUID) (remediation.Snapshot, error) {
	var snap remediation.Snapshot
	err := r.gate(func(d *data) error {
		rem, ok := d.remediations[id]
		if !ok {
			return repository.ErrRemediationNotFound
		}
		var err error
		snap, err = d.snapshot(rem)
		return err
	})
	return snap, err
}

// verifying reports whether a remediation's current try is verifying.
func (d *data) verifying(rem *remediation.Remediation) bool {
	t := d.currentTry(rem)
	return t != nil && t.Status == remediation.TryVerifying
}

func (r *remediationRepository) ClaimNext(_ context.Context, owner string, now time.Time, lease time.Duration) (uuid.UUID, bool, error) {
	var (
		id    uuid.UUID
		found bool
	)
	err := r.gate(func(d *data) error {
		for _, rem := range d.sortedRemediations() {
			if rem.Status != remediation.StatusRunning || !leaseFree(rem, now) || d.verifying(rem) {
				continue
			}
			until := now.Add(lease)
			rem.LeaseOwner, rem.LeaseUntil = owner, &until
			id, found = rem.ID, true
			return nil
		}
		return nil
	})
	return id, found, err
}

func (r *remediationRepository) Claim(_ context.Context, id uuid.UUID, owner string, now time.Time, lease time.Duration) (bool, error) {
	var claimed bool
	err := r.gate(func(d *data) error {
		rem, ok := d.remediations[id]
		if !ok || rem.Status != remediation.StatusRunning || !leaseFree(rem, now) {
			return nil
		}
		until := now.Add(lease)
		rem.LeaseOwner, rem.LeaseUntil = owner, &until
		claimed = true
		return nil
	})
	return claimed, err
}

func (r *remediationRepository) Hold(_ context.Context, id uuid.UUID, owner string, now time.Time, lease time.Duration) error {
	return r.gate(func(d *data) error {
		rem, ok := d.remediations[id]
		if !ok || rem.LeaseOwner != owner || rem.LeaseUntil == nil || rem.LeaseUntil.Before(now) {
			return repository.ErrLeaseLost
		}
		until := now.Add(lease)
		rem.LeaseUntil = &until
		rem.UpdatedAt = now
		return nil
	})
}

func (r *remediationRepository) ReleaseLease(_ context.Context, id uuid.UUID, owner string) error {
	return r.gate(func(d *data) error {
		if rem, ok := d.remediations[id]; ok && rem.LeaseOwner == owner {
			rem.LeaseOwner, rem.LeaseUntil = "", nil
		}
		return nil
	})
}

func (r *remediationRepository) SaveRemediation(_ context.Context, rem remediation.Remediation) error {
	return r.gate(func(d *data) error {
		if err := validStatus("remediation", rem.Status, remediation.AllRemediationStatuses()); err != nil {
			return err
		}
		stored, ok := d.remediations[rem.ID]
		if !ok {
			return fmt.Errorf("save remediation %s: %w", rem.ID, repository.ErrRemediationNotFound)
		}
		stored.Flow = rem.Flow
		stored.Status = rem.Status
		stored.TriggerPayload = slices.Clone(rem.TriggerPayload)
		if len(stored.TriggerPayload) == 0 {
			stored.TriggerPayload = []byte("{}")
		}
		stored.TriggerMessageID = rem.TriggerMessageID
		stored.TriggerFields = cloneRemediation(rem).TriggerFields
		if len(stored.TriggerFields) == 0 {
			stored.TriggerFields = nil
		}
		stored.CurrentTry = rem.CurrentTry
		stored.Failure = rem.Failure
		stored.UpdatedAt = orNow(rem.UpdatedAt)
		return nil
	})
}

func (r *remediationRepository) SaveTry(_ context.Context, t remediation.Try) error {
	return r.gate(func(d *data) error {
		if err := validStatus("try", t.Status, remediation.AllTryStatuses()); err != nil {
			return err
		}
		if _, ok := d.remediations[t.RemediationID]; !ok {
			return fmt.Errorf("save try: remediation %s does not exist", t.RemediationID)
		}
		if t.Try < 1 {
			return fmt.Errorf("try %d violates its check constraint", t.Try)
		}
		stored := cloneTry(t)
		if len(stored.FileRevisions) == 0 {
			stored.FileRevisions = nil
		}
		stored.UpdatedAt = orNow(stored.UpdatedAt)
		key := tryKey{t.RemediationID, t.Try}
		if old, ok := d.tries[key]; ok {
			stored.CreatedAt = old.CreatedAt
		} else {
			stored.CreatedAt = orNow(stored.CreatedAt)
		}
		d.tries[key] = &stored
		return nil
	})
}

func (r *remediationRepository) SaveCluster(_ context.Context, c remediation.Cluster) error {
	return r.gate(func(d *data) error {
		if err := validStatus("cluster", c.Status, remediation.AllClusterStatuses()); err != nil {
			return err
		}
		if _, ok := d.tries[tryKey{c.RemediationID, c.Try}]; !ok {
			return fmt.Errorf("save cluster: try %d of remediation %s does not exist", c.Try, c.RemediationID)
		}
		if !slices.Contains([]workflow.ClusterKind{workflow.ClusterSingle, workflow.ClusterSharedUpstream, workflow.ClusterCrossService}, c.Kind) {
			return fmt.Errorf("cluster kind %q violates its check constraint", c.Kind)
		}
		if c.Cluster < 1 {
			return fmt.Errorf("cluster %d violates its check constraint", c.Cluster)
		}
		stored := cloneCluster(c)
		if len(stored.Members) == 0 {
			stored.Members = nil
		}
		stored.UpdatedAt = orNow(stored.UpdatedAt)
		d.clusters[clusterKey{c.RemediationID, c.Try, c.Cluster}] = &stored
		return nil
	})
}

func (r *remediationRepository) SaveStep(_ context.Context, s remediation.StepRun) error {
	return r.gate(func(d *data) error {
		if err := validStatus("step", s.Status, remediation.AllStepStatuses()); err != nil {
			return err
		}
		if _, ok := d.tries[tryKey{s.RemediationID, s.Try}]; !ok {
			return fmt.Errorf("save step: try %d of remediation %s does not exist", s.Try, s.RemediationID)
		}
		if !slices.Contains(stepKinds, s.Kind) {
			return fmt.Errorf("step kind %q violates its check constraint", s.Kind)
		}
		stored := cloneStep(s)
		if len(stored.Output) == 0 {
			stored.Output = nil
		}
		if len(stored.Findings) == 0 {
			stored.Findings = nil
		}
		if len(stored.WaitingOn) == 0 {
			stored.WaitingOn = nil
		}
		stored.StartedAt = orNow(stored.StartedAt)
		for _, old := range d.steps {
			if old.RemediationID == s.RemediationID && old.Try == s.Try && old.Cluster == s.Cluster &&
				old.Step == s.Step && old.Round == s.Round {
				stored.ID = old.ID
				*old = stored
				return nil
			}
		}
		d.lastStepID++
		stored.ID = d.lastStepID
		d.steps = append(d.steps, &stored)
		return nil
	})
}

func (r *remediationRepository) ListVerifying(_ context.Context, limit int) ([]remediation.Snapshot, error) {
	var out []remediation.Snapshot
	err := r.gate(func(d *data) error {
		for _, rem := range d.sortedRemediations() {
			if limit > 0 && len(out) == limit {
				break
			}
			if rem.Status == remediation.StatusRunning && d.verifying(rem) {
				snap, err := d.snapshot(rem)
				if err != nil {
					return err
				}
				out = append(out, snap)
			}
		}
		return nil
	})
	return out, err
}

func (r *remediationRepository) ListPastDeadline(_ context.Context, now time.Time, limit int) ([]uuid.UUID, error) {
	type late struct {
		id       uuid.UUID
		deadline time.Time
	}
	var rows []late
	err := r.gate(func(d *data) error {
		for _, rem := range d.remediations {
			t := d.currentTry(rem)
			if rem.Status != remediation.StatusRunning || t == nil || !t.DeadlineAt.Before(now) {
				continue
			}
			switch t.Status {
			case remediation.TryDrafting, remediation.TryChecking, remediation.TryVerifying:
				rows = append(rows, late{rem.ID, t.DeadlineAt})
			}
		}
		return nil
	})
	sort.Slice(rows, func(i, j int) bool {
		if !rows[i].deadline.Equal(rows[j].deadline) {
			return rows[i].deadline.Before(rows[j].deadline)
		}
		return rows[i].id.String() < rows[j].id.String()
	})
	if limit > 0 && len(rows) > limit {
		rows = rows[:limit]
	}
	var ids []uuid.UUID
	for _, row := range rows {
		ids = append(ids, row.id)
	}
	return ids, err
}

func (r *remediationRepository) DeleteFinishedBefore(_ context.Context, cutoff time.Time) (int64, error) {
	var n int64
	err := r.gate(func(d *data) error {
		for id, rem := range d.remediations {
			if (rem.Status != remediation.StatusDone && rem.Status != remediation.StatusSystemFailure) || !rem.UpdatedAt.Before(cutoff) {
				continue
			}
			delete(d.remediations, id)
			for key := range d.tries {
				if key.remediationID == id {
					delete(d.tries, key)
				}
			}
			for key := range d.clusters {
				if key.remediationID == id {
					delete(d.clusters, key)
				}
			}
			d.steps = slices.DeleteFunc(d.steps, func(s *remediation.StepRun) bool { return s.RemediationID == id })
			for _, row := range d.proposals {
				if row.p.RemediationID == id.String() {
					row.p.RemediationID = ""
				}
			}
			n++
		}
		return nil
	})
	return n, err
}
