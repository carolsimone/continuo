package runner

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/domain/repository"
	"github.com/carolsimone/continuo/agent-remediation/serialization"
	"github.com/carolsimone/continuo/agent-remediation/service/uow"
)

// The journal holds one row per run of a step: per try, cluster, step and
// round. A row records the step's input digest, its output, its findings, how
// long it took and, for a model call, the model, the prompt version and the
// tokens. The output is one of the journal records of package serialization,
// which also computes the input digest. A cluster's evidence is rebuilt from
// the outputs of its done rows whenever a step runs, so a step that runs after
// a restart sees exactly what it would have seen without one.

// The kinds of journal row the runner writes besides a step's own Kind.
const (
	// kindRunner is a step the runner itself performs.
	kindRunner = "runner"
	// kindOutput is an output step.
	kindOutput = "output"
)

// The runner's own steps of a cluster.
const (
	// stepPlan finds the steps a cluster runs from its type. It has a row only
	// when it failed: its type is not among the loaded files.
	stepPlan = "plan"
	// stepPreflight runs a cluster's pre-flight checks in one pass. Its row's
	// output holds the draft as the pass left it: with every edit's original
	// filled and, once every check passed, the URIs of its stored content and
	// diff. Its findings are every finding of the pass.
	stepPreflight = "preflight"
	// stepDrafts reads the checked drafts of a try back from the journal once
	// every cluster has ended. It is a flow-level row, and has one only when
	// that failed.
	stepDrafts = "drafts"
)

type stepKey struct {
	cluster int
	step    string
	round   int
}

// journal is the step rows of one try, by cluster, step and round.
type journal map[stepKey]remediation.StepRun

func newJournal(steps []remediation.StepRun) journal {
	j := make(journal, len(steps))
	for _, s := range steps {
		j[stepKey{cluster: s.Cluster, step: s.Step, round: s.Round}] = s
	}
	return j
}

// row returns the row of one step of one cluster in one round.
func (j journal) row(cluster int, step string, round int) (remediation.StepRun, bool) {
	s, ok := j[stepKey{cluster: cluster, step: step, round: round}]
	return s, ok
}

// done returns the row of a step that finished, and false when the step has no
// row or its last run did not finish.
func (j journal) done(cluster int, step string, round int) (remediation.StepRun, bool) {
	s, ok := j.row(cluster, step, round)
	return s, ok && s.Status == remediation.StepDone
}

// newRow starts the row of one run of a step of the current try. It keeps the
// failures earlier runs of the same step recorded.
func (r *Runner) newRow(run tryRun, cluster int, step, kind string, round int, revision string) remediation.StepRun {
	row := remediation.StepRun{
		RemediationID: run.snap.Remediation.ID,
		Try:           run.snap.Try.Try,
		Cluster:       cluster,
		Step:          step,
		Kind:          kind,
		Round:         round,
		FileRevision:  revision,
		StartedAt:     r.clock.Now(),
	}
	if prev, ok := run.journal.row(cluster, step, round); ok {
		row.Failures = prev.Failures
	}
	return row
}

// finish marks row done now.
func (r *Runner) finish(row *remediation.StepRun) {
	now := r.clock.Now()
	row.Status = remediation.StepDone
	row.FinishedAt = &now
}

// recordFailure records that a run of a step failed with cause, in a
// transaction of its own after holding the lease: the row is failed, its
// failures go up by one and its last error is cause, without NUL characters,
// which a text column refuses. A failed run stores no output and no findings:
// only a done row is ever read back, and whatever made its commit fail is not
// written again. It returns cause, so the drive stops with it and the next
// drive runs the step again. A lost lease is returned as it is, and nothing is
// recorded for a drive whose context ended. A step that ran past its limit is
// recorded, with a cause that says so.
func (r *Runner) recordFailure(ctx context.Context, row remediation.StepRun, cause error) error {
	if (ctx.Err() != nil && !stepTimedOut(ctx)) || errors.Is(cause, repository.ErrLeaseLost) {
		return cause
	}
	now := r.clock.Now()
	row.Status = remediation.StepFailed
	row.Output = nil
	row.Findings = nil
	row.FinishedAt = &now
	return r.countFailure(ctx, row, cause)
}

// countFailure raises the row's failures by one and records cause as its last
// error, without NUL characters, leaving the rest of the row as it is, in a
// transaction of its own after holding the lease. It returns cause. A lost
// lease is returned as it is, and nothing is recorded for a drive whose context
// ended. A step that ran past its limit is recorded, with a cause that says so,
// in a context that is not ended.
func (r *Runner) countFailure(ctx context.Context, row remediation.StepRun, cause error) error {
	if errors.Is(cause, repository.ErrLeaseLost) {
		return cause
	}
	if ctx.Err() != nil {
		if !stepTimedOut(ctx) {
			return cause
		}
		cause = fmt.Errorf("the step ran longer than %s and was stopped: %w", r.stepLimit(), cause)
		ctx = context.WithoutCancel(ctx)
	}
	row.Failures++
	row.LastError = strings.ReplaceAll(cause.Error(), "\x00", "")
	err := r.commit(ctx, row.RemediationID, func(u uow.UnitOfWork) error {
		return u.RemediationRepo().SaveStep(ctx, row)
	})
	switch {
	case errors.Is(err, repository.ErrLeaseLost):
		return err
	case err != nil:
		return errors.Join(cause, fmt.Errorf("record the failure of step %s: %w", row.Step, err))
	}
	r.logger.Warn("remediation step failed; the next drive runs it again",
		"remediation", row.RemediationID, "try", row.Try, "cluster", row.Cluster, "step", row.Step,
		"failures", row.Failures, "error", cause)
	return cause
}

// commitStep commits the rows of a step run and the clusters it changed, in
// one transaction after holding the lease, and clears the failures run's
// remediation counted in a row before it (clearFailures). A commit that fails
// is recorded as a failure of blame, the row of the step that ran.
func (r *Runner) commitStep(ctx context.Context, run tryRun, blame remediation.StepRun, rows []remediation.StepRun, clusters []remediation.Cluster) error {
	now := r.clock.Now()
	err := r.commit(ctx, blame.RemediationID, func(u uow.UnitOfWork) error {
		if err := r.clearFailures(ctx, u, run); err != nil {
			return err
		}
		repo := u.RemediationRepo()
		for _, row := range rows {
			if err := repo.SaveStep(ctx, row); err != nil {
				return fmt.Errorf("save step %s: %w", row.Step, err)
			}
		}
		for _, c := range clusters {
			c.UpdatedAt = now
			if err := repo.SaveCluster(ctx, c); err != nil {
				return fmt.Errorf("save cluster %d: %w", c.Cluster, err)
			}
		}
		return nil
	})
	if err != nil {
		return r.recordFailure(ctx, blame, err)
	}
	return nil
}

// encodeEvidence encodes each piece of evidence a step gathered on its own,
// each by the Go type the registry gives its key.
func (r *Runner) encodeEvidence(values map[remediation.EvidenceKey]any) (serialization.EncodedEvidence, error) {
	return serialization.EncodeEvidence(values, r.d.Registry.EvidenceType)
}

// withEvidence returns ev with every encoded piece of evidence added, each
// decoded into the Go type the registry gives its key, in key order.
func (r *Runner) withEvidence(ev remediation.Evidence, encoded serialization.EncodedEvidence) (remediation.Evidence, error) {
	return encoded.AddTo(ev, r.d.Registry.EvidenceType)
}

// contextEvidence returns ev with the evidence a done context step's row
// recorded.
func (r *Runner) contextEvidence(ev remediation.Evidence, row remediation.StepRun) (remediation.Evidence, error) {
	rec, err := serialization.DecodeRecord[serialization.ContextRecord](row.Output)
	if err != nil {
		return ev, fmt.Errorf("decode the output of step %s: %w", row.Step, err)
	}
	return r.withEvidence(ev, rec.Evidence)
}

// promptDraft returns the draft a done prompt step's row recorded.
func promptDraft(row remediation.StepRun) (remediation.Draft, error) {
	rec, err := serialization.DecodeRecord[serialization.PromptRecord](row.Output)
	if err != nil {
		return remediation.Draft{}, fmt.Errorf("decode the output of step %s: %w", row.Step, err)
	}
	return rec.Draft, nil
}

// preflightDraft returns the draft a done pre-flight pass's row recorded.
func preflightDraft(row remediation.StepRun) (remediation.Draft, error) {
	rec, err := serialization.DecodeRecord[serialization.PreflightRecord](row.Output)
	if err != nil {
		return remediation.Draft{}, fmt.Errorf("decode the output of step %s: %w", row.Step, err)
	}
	return rec.Draft, nil
}

// checkResult returns the result and the evidence a done pre-flight check's
// row recorded.
func checkResult(row remediation.StepRun) (serialization.CheckRecord, error) {
	rec, err := serialization.DecodeRecord[serialization.CheckRecord](row.Output)
	if err != nil {
		return serialization.CheckRecord{}, fmt.Errorf("decode the output of step %s: %w", row.Step, err)
	}
	return rec, nil
}
