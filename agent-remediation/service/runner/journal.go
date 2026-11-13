package runner

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"

	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/domain/repository"
	"github.com/carolsimone/continuo/agent-remediation/service/uow"
)

// The journal holds one row per run of a step: per try, cluster, step and
// round. A row records the step's input digest, its output as JSON, its
// findings, how long it took and, for a model call, the model, the prompt
// version and the tokens. A cluster's evidence is rebuilt from the outputs of
// its done rows whenever a step runs, so a step that runs after a restart sees
// exactly what it would have seen without one.

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

// contextRecord is a context step's output as its row stores it: each piece of
// evidence encoded on its own, so it decodes into its key's registered type.
type contextRecord struct {
	Evidence map[remediation.EvidenceKey]json.RawMessage `json:"evidence,omitempty"`
	Declined bool                                        `json:"declined,omitempty"`
	Decline  string                                      `json:"decline,omitempty"`
}

// promptRecord is a prompt step's output as its row stores it.
type promptRecord struct {
	Draft    remediation.Draft `json:"draft"`
	Declined bool              `json:"declined,omitempty"`
	Decline  string            `json:"decline,omitempty"`
}

// checkRecord is a pre-flight check's output as its row stores it.
type checkRecord struct {
	Result   remediation.CheckResult                     `json:"result"`
	Evidence map[remediation.EvidenceKey]json.RawMessage `json:"evidence,omitempty"`
}

// preflightRecord is the output of the runner's pre-flight pass.
type preflightRecord struct {
	Draft remediation.Draft `json:"draft"`
}

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
// recorded for a drive whose context ended.
func (r *Runner) recordFailure(ctx context.Context, row remediation.StepRun, cause error) error {
	if ctx.Err() != nil || errors.Is(cause, repository.ErrLeaseLost) {
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
// ended.
func (r *Runner) countFailure(ctx context.Context, row remediation.StepRun, cause error) error {
	if ctx.Err() != nil || errors.Is(cause, repository.ErrLeaseLost) {
		return cause
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
// one transaction after holding the lease. A commit that fails is recorded as
// a failure of blame, the row of the step that ran.
func (r *Runner) commitStep(ctx context.Context, blame remediation.StepRun, rows []remediation.StepRun, clusters []remediation.Cluster) error {
	now := r.clock.Now()
	err := r.commit(ctx, blame.RemediationID, func(u uow.UnitOfWork) error {
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

// encodeEvidence encodes each piece of evidence a step gathered on its own. It
// refuses a key with no registered type, since its value could not be decoded
// again.
func (r *Runner) encodeEvidence(values map[remediation.EvidenceKey]any) (map[remediation.EvidenceKey]json.RawMessage, error) {
	if len(values) == 0 {
		return nil, nil
	}
	out := make(map[remediation.EvidenceKey]json.RawMessage, len(values))
	for k, v := range values {
		if _, ok := r.d.Registry.EvidenceType(k); !ok {
			return nil, fmt.Errorf("evidence %q has no registered type", k)
		}
		raw, err := json.Marshal(v)
		if err != nil {
			return nil, fmt.Errorf("encode evidence %q: %w", k, err)
		}
		out[k] = raw
	}
	return out, nil
}

// withEvidence returns ev with every encoded piece of evidence added, each
// decoded into the Go type the registry gives its key, in key order.
func (r *Runner) withEvidence(ev remediation.Evidence, raw map[remediation.EvidenceKey]json.RawMessage) (remediation.Evidence, error) {
	keys := make([]remediation.EvidenceKey, 0, len(raw))
	for k := range raw {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	for _, k := range keys {
		zero, ok := r.d.Registry.EvidenceType(k)
		if !ok {
			return ev, fmt.Errorf("evidence %q has no registered type", k)
		}
		ptr := reflect.New(reflect.TypeOf(zero()))
		if err := json.Unmarshal(raw[k], ptr.Interface()); err != nil {
			return ev, fmt.Errorf("decode evidence %q: %w", k, err)
		}
		ev = ev.With(k, ptr.Elem().Interface())
	}
	return ev, nil
}

// contextEvidence returns ev with the evidence a done context step's row
// recorded.
func (r *Runner) contextEvidence(ev remediation.Evidence, row remediation.StepRun) (remediation.Evidence, error) {
	var rec contextRecord
	if err := json.Unmarshal(row.Output, &rec); err != nil {
		return ev, fmt.Errorf("decode the output of step %s: %w", row.Step, err)
	}
	return r.withEvidence(ev, rec.Evidence)
}

// promptDraft returns the draft a done prompt step's row recorded.
func promptDraft(row remediation.StepRun) (remediation.Draft, error) {
	var rec promptRecord
	if err := json.Unmarshal(row.Output, &rec); err != nil {
		return remediation.Draft{}, fmt.Errorf("decode the output of step %s: %w", row.Step, err)
	}
	return rec.Draft, nil
}

// preflightDraft returns the draft a done pre-flight pass's row recorded.
func preflightDraft(row remediation.StepRun) (remediation.Draft, error) {
	var rec preflightRecord
	if err := json.Unmarshal(row.Output, &rec); err != nil {
		return remediation.Draft{}, fmt.Errorf("decode the output of step %s: %w", row.Step, err)
	}
	return rec.Draft, nil
}

// checkResult returns the result and the evidence a done pre-flight check's
// row recorded.
func checkResult(row remediation.StepRun) (checkRecord, error) {
	var rec checkRecord
	if err := json.Unmarshal(row.Output, &rec); err != nil {
		return checkRecord{}, fmt.Errorf("decode the output of step %s: %w", row.Step, err)
	}
	return rec, nil
}

// evidenceValues is every piece of evidence ev holds, by key.
func evidenceValues(ev remediation.Evidence) map[remediation.EvidenceKey]any {
	out := make(map[remediation.EvidenceKey]any, len(ev.Keys()))
	for _, k := range ev.Keys() {
		v, _ := remediation.Get[any](ev, k)
		out[k] = v
	}
	return out
}

// inputDigest identifies what a step ran on: the cluster's evidence and the
// round. Two runs with the same digest were given the same input.
func inputDigest(ev remediation.Evidence, round int) (string, error) {
	raw, err := json.Marshal(struct {
		Evidence map[remediation.EvidenceKey]any `json:"evidence"`
		Round    int                             `json:"round"`
	}{evidenceValues(ev), round})
	if err != nil {
		return "", fmt.Errorf("encode the step's input: %w", err)
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}
