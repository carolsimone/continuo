// Package model holds execution-controller's domain aggregates.
package model

import (
	"fmt"
	"time"

	"github.com/carolsimone/continuo/execution-controller/domain/command"
	pkgmodel "github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/google/uuid"
)

// defaultMaxRetries is the deploy-attempt budget for a new Deployment.
const defaultMaxRetries = 3

// Mode is the dispatch path that produced this Deployment. A production
// deploy is one attempt of a task, from task.dispatch:v1; validation, seed-build
// and compile deploys originate from the candidate-release flow and carry a
// per-node terminal outcome.
type Mode string

const (
	ModeProduction Mode = "production"
	ModeValidation Mode = "validation"
	ModeSeedBuild  Mode = "seed_build"
	ModeCompile    Mode = "compile"
)

// BackoffPolicy computes the delay before the next deploy attempt:
// base * 2^retryCount, capped at Cap.
type BackoffPolicy struct {
	Base time.Duration
	Cap  time.Duration
}

func (b BackoffPolicy) delay(retryCount int) time.Duration {
	d := b.Base << retryCount
	if d <= 0 || d > b.Cap { // d<=0 guards shift overflow
		return b.Cap
	}
	return d
}

// Deployment is the aggregate for one queued K8s deploy. It owns its lifecycle
// transitions and retry policy; persistence and Kubernetes are adapter concerns.
type Deployment struct {
	id                  uuid.UUID
	messageProcessingID *uuid.UUID
	mode                Mode
	command             command.DeployTask           // populated only when mode == ModeProduction
	validationCmd       command.ValidationDeployTask // populated only when mode == ModeValidation
	status              Status
	retryCount          int
	maxRetries          int
	nextAttemptAt       time.Time
	createdAt           time.Time
	deployedAt          *time.Time
	errorMessage        *string

	// Terminal outcome: attached by RecordOutcome (validation, seed-build,
	// compile) or Finish (production). The dbt artifact fields are populated
	// only by RecordOutcome.
	outcome          string
	dbtLogURI        string
	dbtRunResultsURI string
	failedContainer  string
	outcomeAt        *time.Time

	// facts is what has been observed of the attempt's Job so far.
	facts AttemptFacts
	// lastReport is the report last emitted for a production attempt.
	lastReport *Report
}

// NewDeployment starts a fresh pending Deployment due immediately.
func NewDeployment(cmd command.DeployTask, msgProcID *uuid.UUID, now time.Time) *Deployment {
	return &Deployment{
		id:                  uuid.New(),
		messageProcessingID: msgProcID,
		mode:                ModeProduction,
		command:             cmd,
		status:              StatusPending,
		maxRetries:          defaultMaxRetries,
		nextAttemptAt:       now,
		createdAt:           now,
	}
}

// NewValidationDeployment starts a fresh validation Deployment. When
// hasUpstreams is true the deployment begins in StatusBlocked, waiting for all
// in-set intra-service upstreams to succeed before the dispatcher can pick it
// up. When false (root node in the build set) it starts StatusPending and is
// eligible for dispatch immediately.
func NewValidationDeployment(cmd command.ValidationDeployTask, msgProcID *uuid.UUID, now time.Time, hasUpstreams bool) *Deployment {
	status := StatusPending
	if hasUpstreams {
		status = StatusBlocked
	}
	return &Deployment{
		id:                  uuid.New(),
		messageProcessingID: msgProcID,
		mode:                ModeValidation,
		validationCmd:       cmd,
		status:              status,
		maxRetries:          defaultMaxRetries,
		nextAttemptAt:       now,
		createdAt:           now,
	}
}

// NewSeedBuildDeployment creates a seed-build deployment: a candidate seed built
// with the team image (dbt seed) into the candidate schema. Seeds are dbt roots
// (no in-leg upstreams), so the deployment always starts pending. It reuses the
// ValidationDeployTask command shape and the outcome columns.
func NewSeedBuildDeployment(cmd command.ValidationDeployTask, msgProcID *uuid.UUID, now time.Time) *Deployment {
	return &Deployment{
		id:                  uuid.New(),
		messageProcessingID: msgProcID,
		mode:                ModeSeedBuild,
		validationCmd:       cmd,
		status:              StatusPending,
		maxRetries:          defaultMaxRetries,
		nextAttemptAt:       now,
		createdAt:           now,
	}
}

// NewCompileDeployment creates a compile deployment: the changed service's dbt
// manifest is compiled into S3 before validation runs. Compile is a single root
// node (no intra-service upstreams), so the deployment always starts pending.
// It reuses the ValidationDeployTask command shape and the outcome columns.
func NewCompileDeployment(cmd command.ValidationDeployTask, msgProcID *uuid.UUID, now time.Time) *Deployment {
	return &Deployment{
		id:                  uuid.New(),
		messageProcessingID: msgProcID,
		mode:                ModeCompile,
		validationCmd:       cmd,
		status:              StatusPending,
		maxRetries:          defaultMaxRetries,
		nextAttemptAt:       now,
		createdAt:           now,
	}
}

// Reconstitute rebuilds a production Deployment that has no recorded outcome
// from persisted state.
func Reconstitute(
	id uuid.UUID,
	msgProcID *uuid.UUID,
	cmd command.DeployTask,
	status Status,
	retryCount, maxRetries int,
	nextAttemptAt, createdAt time.Time,
	deployedAt *time.Time,
	errorMessage *string,
) *Deployment {
	return ReconstituteProduction(id, msgProcID, cmd, status, retryCount, maxRetries,
		nextAttemptAt, createdAt, deployedAt, errorMessage, "", nil, AttemptFacts{})
}

// ReconstituteProduction rebuilds a production Deployment from persisted state,
// including the outcome its attempt recorded. Adapters use this for rows whose
// mode == production.
func ReconstituteProduction(
	id uuid.UUID,
	msgProcID *uuid.UUID,
	cmd command.DeployTask,
	status Status,
	retryCount, maxRetries int,
	nextAttemptAt, createdAt time.Time,
	deployedAt *time.Time,
	errorMessage *string,
	outcome string,
	outcomeAt *time.Time,
	facts AttemptFacts,
) *Deployment {
	return &Deployment{
		id:                  id,
		messageProcessingID: msgProcID,
		mode:                ModeProduction,
		command:             cmd,
		status:              status,
		retryCount:          retryCount,
		maxRetries:          maxRetries,
		nextAttemptAt:       nextAttemptAt,
		createdAt:           createdAt,
		deployedAt:          deployedAt,
		errorMessage:        errorMessage,
		outcome:             outcome,
		outcomeAt:           outcomeAt,
		facts:               facts,
	}
}

// ReconstituteValidation rebuilds a validation-mode Deployment from persisted
// state, including its terminal outcome columns. Adapters use this for rows
// whose mode == validation.
func ReconstituteValidation(
	id uuid.UUID,
	msgProcID *uuid.UUID,
	cmd command.ValidationDeployTask,
	status Status,
	retryCount, maxRetries int,
	nextAttemptAt, createdAt time.Time,
	deployedAt *time.Time,
	errorMessage *string,
	outcome, dbtLogURI, runResultsURI, failedContainer string,
	outcomeAt *time.Time,
	facts AttemptFacts,
) *Deployment {
	return &Deployment{
		id:                  id,
		messageProcessingID: msgProcID,
		mode:                ModeValidation,
		validationCmd:       cmd,
		status:              status,
		retryCount:          retryCount,
		maxRetries:          maxRetries,
		nextAttemptAt:       nextAttemptAt,
		createdAt:           createdAt,
		deployedAt:          deployedAt,
		errorMessage:        errorMessage,
		outcome:             outcome,
		dbtLogURI:           dbtLogURI,
		dbtRunResultsURI:    runResultsURI,
		failedContainer:     failedContainer,
		outcomeAt:           outcomeAt,
		facts:               facts,
	}
}

// ReconstituteSeedBuild rebuilds a seed-build-mode Deployment from persisted
// state. It mirrors ReconstituteValidation but sets mode: ModeSeedBuild.
func ReconstituteSeedBuild(
	id uuid.UUID,
	msgProcID *uuid.UUID,
	cmd command.ValidationDeployTask,
	status Status,
	retryCount, maxRetries int,
	nextAttemptAt, createdAt time.Time,
	deployedAt *time.Time,
	errorMessage *string,
	outcome, dbtLogURI, runResultsURI, failedContainer string,
	outcomeAt *time.Time,
	facts AttemptFacts,
) *Deployment {
	return &Deployment{
		id:                  id,
		messageProcessingID: msgProcID,
		mode:                ModeSeedBuild,
		validationCmd:       cmd,
		status:              status,
		retryCount:          retryCount,
		maxRetries:          maxRetries,
		nextAttemptAt:       nextAttemptAt,
		createdAt:           createdAt,
		deployedAt:          deployedAt,
		errorMessage:        errorMessage,
		outcome:             outcome,
		dbtLogURI:           dbtLogURI,
		dbtRunResultsURI:    runResultsURI,
		failedContainer:     failedContainer,
		outcomeAt:           outcomeAt,
		facts:               facts,
	}
}

// ReconstituteCompile rebuilds a compile-mode Deployment from persisted state.
// It mirrors ReconstituteSeedBuild but sets mode: ModeCompile.
func ReconstituteCompile(
	id uuid.UUID,
	msgProcID *uuid.UUID,
	cmd command.ValidationDeployTask,
	status Status,
	retryCount, maxRetries int,
	nextAttemptAt, createdAt time.Time,
	deployedAt *time.Time,
	errorMessage *string,
	outcome, dbtLogURI, runResultsURI, failedContainer string,
	outcomeAt *time.Time,
	facts AttemptFacts,
) *Deployment {
	return &Deployment{
		id:                  id,
		messageProcessingID: msgProcID,
		mode:                ModeCompile,
		validationCmd:       cmd,
		status:              status,
		retryCount:          retryCount,
		maxRetries:          maxRetries,
		nextAttemptAt:       nextAttemptAt,
		createdAt:           createdAt,
		deployedAt:          deployedAt,
		errorMessage:        errorMessage,
		outcome:             outcome,
		dbtLogURI:           dbtLogURI,
		dbtRunResultsURI:    runResultsURI,
		failedContainer:     failedContainer,
		outcomeAt:           outcomeAt,
		facts:               facts,
	}
}

// IsDeployable reports whether the command carries the identity and target a
// deploy needs. A row whose job_params could not be deserialized recovers only
// its task/schedule identity, so this returns false and the dispatcher fails it
// permanently rather than attempting a meaningless deploy.
func (d *Deployment) IsDeployable() bool {
	if d.mode == ModeValidation || d.mode == ModeSeedBuild {
		return d.validationCmd.JobName != "" &&
			d.validationCmd.NodeID != "" &&
			d.validationCmd.ReleaseID != "" &&
			d.validationCmd.NodeType != "" &&
			d.validationCmd.ImageTag != ""
	}
	if d.mode == ModeCompile {
		// Compile jobs have no NodeType — they compile the full manifest for a
		// service, not a single dbt node. Only identity + image are required.
		return d.validationCmd.JobName != "" &&
			d.validationCmd.NodeID != "" &&
			d.validationCmd.ReleaseID != "" &&
			d.validationCmd.ImageTag != ""
	}
	return d.command.JobName != "" &&
		d.command.TaskID != "" &&
		d.command.ScheduleID != "" &&
		d.command.NodeType != ""
}

// moveTo changes the deployment's status when the state machine allows it.
func (d *Deployment) moveTo(next Status) error {
	if !CanMove(d.status, next) {
		return fmt.Errorf("deployment %s cannot move from %q to %q", d.id, d.status, next)
	}
	d.status = next
	return nil
}

// Reserve takes an execution slot for a pending deployment.
func (d *Deployment) Reserve() error { return d.moveTo(StatusReserved) }

// MarkStarted records that the reserved deployment's Job was created.
// deployed_at keeps the creation time.
func (d *Deployment) MarkStarted(now time.Time) error {
	if err := d.moveTo(StatusStarting); err != nil {
		return err
	}
	d.deployedAt = &now
	d.errorMessage = nil
	return nil
}

// MarkRunning records that the Job's main container started. It reports
// whether the status changed; a running deployment stays running.
func (d *Deployment) MarkRunning() (bool, error) {
	if d.status == StatusRunning {
		return false, nil
	}
	if err := d.moveTo(StatusRunning); err != nil {
		return false, err
	}
	return true, nil
}

// settle records a terminal outcome and ends the deployment done; its Job
// exists (starting, running or terminating).
func (d *Deployment) settle(outcome string, now time.Time) error {
	if d.outcomeAt != nil {
		return fmt.Errorf("outcome already recorded for deployment %s", d.id)
	}
	if !d.AwaitingResult() {
		return fmt.Errorf("deployment %s has no Job to settle in status %q", d.id, d.status)
	}
	if err := d.moveTo(StatusDone); err != nil {
		return err
	}
	d.outcome = outcome
	ts := now
	d.outcomeAt = &ts
	return nil
}

// Finish records a production Job's terminal outcome ("ok", "failed" or
// "stopped") and releases its slot. A recorded outcome marks the Job's result
// as reported, so observing the same Job again writes nothing.
func (d *Deployment) Finish(outcome string, now time.Time) error {
	if d.mode != ModeProduction {
		return fmt.Errorf("Finish called on %s deployment %s; candidates record through RecordOutcome", d.mode, d.id)
	}
	if outcome != "ok" && outcome != "failed" && outcome != OutcomeStopped {
		return fmt.Errorf("invalid outcome %q", outcome)
	}
	return d.settle(outcome, now)
}

// HasOutcome reports whether the deployment's terminal outcome is recorded.
func (d *Deployment) HasOutcome() bool { return d.outcomeAt != nil }

// AwaitingResult reports whether the deployment's attempt was started and its
// result is not recorded yet: it is starting, running or terminating.
func (d *Deployment) AwaitingResult() bool {
	switch d.status {
	case StatusStarting, StatusRunning, StatusTerminating:
		return true
	default:
		return false
	}
}

// Observe adds what an observation shows of the attempt to its facts and
// reports whether it added anything.
func (d *Deployment) Observe(f AttemptFacts) bool {
	merged, changed := d.facts.Merge(f)
	d.facts = merged
	return changed
}

// BeginTermination records why execution-controller ends the attempt itself
// (a start error or the start timeout) and moves the deployment to
// terminating, where it keeps its slot until the attempt's pods are gone. A
// terminating deployment stays as it is: the first reason stands.
func (d *Deployment) BeginTermination(reason pkgmodel.FailureReason, message string) error {
	if d.status == StatusTerminating {
		return nil
	}
	if err := d.moveTo(StatusTerminating); err != nil {
		return err
	}
	d.facts, _ = d.facts.Merge(AttemptFacts{Reason: reason, Message: message})
	return nil
}

// OutcomeStopped is the outcome of an attempt that was stopped on request.
const OutcomeStopped = "stopped"

// StopEffect is what execution-controller does next after a deployment was
// asked to stop.
type StopEffect int

const (
	// StopNone: the deployment is already ending, or has ended; the stop adds
	// nothing beyond being recorded on an open row.
	StopNone StopEffect = iota
	// StopEnded: a pending or reserved deployment, which has no Job, ended
	// done with the outcome stopped; report it now.
	StopEnded
	// StopTerminate: a starting or running deployment moved to terminating;
	// delete its Job once the change is committed.
	StopTerminate
)

// Stop records that state asked the attempt to stop, as the time of the first
// request. A deployment with no Job (pending or reserved) ends done, outcome
// stopped, at once; one whose Job exists (starting or running) moves to
// terminating, where it keeps its slot until its pods are gone; a terminating
// one stays as it is, keeping the reason it was ended for. A deployment that
// has ended records nothing. Only a production deployment, which is one
// attempt of a task, can be stopped.
func (d *Deployment) Stop(now time.Time) (StopEffect, error) {
	if d.mode != ModeProduction {
		return StopNone, fmt.Errorf("Stop called on %s deployment %s; only a task's attempt is stopped", d.mode, d.id)
	}
	switch d.status {
	case StatusPending, StatusReserved:
		if err := d.moveTo(StatusDone); err != nil {
			return StopNone, err
		}
		d.recordStopRequest(now)
		d.outcome = OutcomeStopped
		ts := now
		d.outcomeAt = &ts
		return StopEnded, nil
	case StatusStarting, StatusRunning:
		if err := d.moveTo(StatusTerminating); err != nil {
			return StopNone, err
		}
		d.recordStopRequest(now)
		return StopTerminate, nil
	case StatusTerminating:
		d.recordStopRequest(now)
	}
	return StopNone, nil
}

func (d *Deployment) recordStopRequest(now time.Time) {
	d.facts, _ = d.facts.Merge(AttemptFacts{StopRequestedAt: &now})
}

// JobName is the name of the Kubernetes Job the deployment creates.
func (d *Deployment) JobName() string {
	if d.mode == ModeProduction {
		return d.command.JobName
	}
	return d.validationCmd.JobName
}

// RetriesLeft reports whether a transient failure of the current deploy try
// leaves another try in the budget: RegisterFailure then returns the
// deployment to pending rather than failing it.
func (d *Deployment) RetriesLeft() bool { return d.retryCount+1 < d.maxRetries }

// RegisterFailure records a failed deploy attempt of a reserved deployment and
// applies the retry policy. A transient failure with attempts left returns the
// deployment to pending (freeing its slot), bumps retryCount and pushes
// nextAttemptAt, and reports terminal=false. Otherwise the deployment fails and
// terminal=true.
func (d *Deployment) RegisterFailure(now time.Time, permanent bool, reason string, backoff BackoffPolicy) (terminal bool, err error) {
	if !permanent && d.RetriesLeft() {
		if err := d.moveTo(StatusPending); err != nil {
			return false, err
		}
		d.nextAttemptAt = now.Add(backoff.delay(d.retryCount))
		d.retryCount++
		d.setError(reason)
		return false, nil
	}
	if err := d.moveTo(StatusFailed); err != nil {
		return false, err
	}
	d.setError(reason)
	return true, nil
}

func (d *Deployment) setError(reason string) {
	msg := reason
	d.errorMessage = &msg
}

// RecordOutcome attaches the terminal outcome to a started (starting, running
// or terminating) validation, seed-build, OR compile
// deployment and releases its slot — all three legs report a per-node terminal
// status the same way: the observer sees the attempt end and records it here
// via outcomes.Recorder. Production deployments announce their result through a
// different path (Finish) and are rejected. Only "ok" and "failed" are accepted.
func (d *Deployment) RecordOutcome(outcome, logURI, runResultsURI, failedContainer string, now time.Time) error {
	if d.mode != ModeValidation && d.mode != ModeSeedBuild && d.mode != ModeCompile {
		return fmt.Errorf("RecordOutcome called on non-validation/seed-build/compile deployment %s", d.id)
	}
	if outcome != "ok" && outcome != "failed" {
		return fmt.Errorf("invalid outcome %q", outcome)
	}
	if err := d.settle(outcome, now); err != nil {
		return err
	}
	d.dbtLogURI = logURI
	d.dbtRunResultsURI = runResultsURI
	d.failedContainer = failedContainer
	return nil
}

// failBeforeStart drives a reserved deployment that cannot be deployed to the
// terminal failed state and records outcome="failed". A deployment that
// RegisterFailure already failed (the terminal branch of a dispatch) skips the
// move.
func (d *Deployment) failBeforeStart(reason string, now time.Time) error {
	if d.outcomeAt != nil {
		return fmt.Errorf("outcome already recorded for deployment %s", d.id)
	}
	if d.status != StatusFailed {
		if err := d.moveTo(StatusFailed); err != nil {
			return err
		}
	}
	d.setError(reason)
	d.outcome = "failed"
	ts := now
	d.outcomeAt = &ts
	return nil
}

// FailValidation drives a validation deployment to a terminal failed state and
// records outcome="failed" in one step. Unlike RecordOutcome it acts on a
// deployment that never started: a validation row that fails BEFORE its Job is
// created (not deployable, or a permanent pre-deploy deployer error) is still
// reserved, yet must reach a terminal "failed" outcome so the per-release
// aggregate can be emitted. It is validation-only and idempotent-safe in that it
// rejects a second recording once an outcome exists.
func (d *Deployment) FailValidation(reason string, now time.Time) error {
	if d.mode != ModeValidation {
		return fmt.Errorf("FailValidation called on non-validation deployment %s", d.id)
	}
	return d.failBeforeStart(reason, now)
}

// FailSeedBuild drives a seed-build deployment to a terminal failed state and
// records outcome="failed" in one step. It is the seed-build equivalent of
// FailValidation: a seed-build row that fails BEFORE its Job is created (not
// deployable, or a permanent pre-deploy deployer error) must reach a terminal
// "failed" outcome so the per-release seed-build aggregate can be emitted.
func (d *Deployment) FailSeedBuild(reason string, now time.Time) error {
	if d.mode != ModeSeedBuild {
		return fmt.Errorf("FailSeedBuild called on non-seed-build deployment %s", d.id)
	}
	return d.failBeforeStart(reason, now)
}

// FailCompile drives a compile deployment to a terminal failed state and
// records outcome="failed" in one step. It is the compile equivalent of
// FailSeedBuild: a compile row that fails BEFORE its Job is created (not
// deployable, or a permanent pre-deploy deployer error) must reach a terminal
// "failed" outcome so the per-release compile aggregate can be emitted.
func (d *Deployment) FailCompile(reason string, now time.Time) error {
	if d.mode != ModeCompile {
		return fmt.Errorf("FailCompile called on non-compile deployment %s", d.id)
	}
	return d.failBeforeStart(reason, now)
}

// Unblock transitions a gated validation deployment from blocked to pending so
// the dispatcher can pick it up. Caller decides readiness (all in-set upstreams
// succeeded); the state machine guards the source state.
func (d *Deployment) Unblock(now time.Time) error {
	if d.mode != ModeValidation {
		return fmt.Errorf("Unblock called on non-validation deployment %s", d.id)
	}
	if err := d.moveTo(StatusPending); err != nil {
		return err
	}
	d.nextAttemptAt = now
	return nil
}

// Skip drives a blocked validation deployment to a terminal skipped state with
// outcome="skipped". Used when an in-set upstream failed, so this node can never
// be validated. Like FailValidation it produces a terminal outcome so the
// per-release aggregate gate counts it (skipped is non-"ok" => release rejected).
func (d *Deployment) Skip(reason string, now time.Time) error {
	if d.mode != ModeValidation {
		return fmt.Errorf("Skip called on non-validation deployment %s", d.id)
	}
	if err := d.moveTo(StatusSkipped); err != nil {
		return err
	}
	d.setError(reason)
	d.outcome = "skipped"
	ts := now
	d.outcomeAt = &ts
	return nil
}

// Accessors used by adapters (persistence) and the application service.
func (d *Deployment) ID() uuid.UUID                   { return d.id }
func (d *Deployment) MessageProcessingID() *uuid.UUID { return d.messageProcessingID }
func (d *Deployment) Mode() Mode                      { return d.mode }
func (d *Deployment) Command() command.DeployTask     { return d.command }

// ValidationCommand is meaningful only when Mode() == ModeValidation,
// ModeSeedBuild, or ModeCompile; for production deployments it returns the zero ValidationDeployTask.
func (d *Deployment) ValidationCommand() command.ValidationDeployTask {
	return d.validationCmd
}

// ReleaseID is meaningful only when Mode() == ModeValidation, ModeSeedBuild,
// or ModeCompile; for production deployments it returns "".
func (d *Deployment) ReleaseID() string { return d.validationCmd.ReleaseID }

// NodeID is meaningful only when Mode() == ModeValidation, ModeSeedBuild,
// or ModeCompile; for production deployments it returns "".
func (d *Deployment) NodeID() string           { return d.validationCmd.NodeID }
func (d *Deployment) Status() Status           { return d.status }
func (d *Deployment) RetryCount() int          { return d.retryCount }
func (d *Deployment) MaxRetries() int          { return d.maxRetries }
func (d *Deployment) NextAttemptAt() time.Time { return d.nextAttemptAt }
func (d *Deployment) CreatedAt() time.Time     { return d.createdAt }
func (d *Deployment) DeployedAt() *time.Time   { return d.deployedAt }
func (d *Deployment) ErrorMessage() *string    { return d.errorMessage }
func (d *Deployment) Outcome() string          { return d.outcome }
func (d *Deployment) DBTLogURI() string        { return d.dbtLogURI }
func (d *Deployment) DBTRunResultsURI() string { return d.dbtRunResultsURI }
func (d *Deployment) FailedContainer() string  { return d.failedContainer }
func (d *Deployment) OutcomeAt() *time.Time    { return d.outcomeAt }

// Facts returns what has been observed of the attempt's Job so far.
func (d *Deployment) Facts() AttemptFacts { return d.facts }
