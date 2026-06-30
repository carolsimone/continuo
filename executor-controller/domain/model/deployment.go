// Package model holds executor-controller's domain aggregates.
package model

import (
	"fmt"
	"time"

	"github.com/carolsimone/continuo/executor-controller/domain/command"
	"github.com/google/uuid"
)

// Status is the lifecycle state of a Deployment.
type Status string

const (
	StatusPending  Status = "pending"
	StatusBlocked  Status = "blocked"
	StatusDeployed Status = "deployed"
	StatusFailed   Status = "failed"
	StatusSkipped  Status = "skipped"
)

// defaultMaxRetries is the deploy-attempt budget for a new Deployment.
const defaultMaxRetries = 3

// Mode is the dispatch path that produced this Deployment. Production deploys
// originate from query.model:v1 / retry.task:v1; validation deploys originate
// from the candidate-release flow and carry a per-node terminal outcome.
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

	// Validation-only terminal outcome, attached by RecordOutcome after dispatch.
	outcome          string
	dbtLogURI        string
	dbtRunResultsURI string
	outcomeAt        *time.Time
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

// Reconstitute rebuilds a Deployment from persisted state. Adapters use this to
// turn a stored row back into an aggregate.
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
	outcome, dbtLogURI, runResultsURI string,
	outcomeAt *time.Time,
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
		outcomeAt:           outcomeAt,
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
	outcome, dbtLogURI, runResultsURI string,
	outcomeAt *time.Time,
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
		outcomeAt:           outcomeAt,
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
	outcome, dbtLogURI, runResultsURI string,
	outcomeAt *time.Time,
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
		outcomeAt:           outcomeAt,
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

// MarkDeployed transitions a pending Deployment to deployed.
func (d *Deployment) MarkDeployed(now time.Time) error {
	if d.status != StatusPending {
		return fmt.Errorf("cannot mark deployed from status %q", d.status)
	}
	d.status = StatusDeployed
	d.deployedAt = &now
	d.errorMessage = nil
	return nil
}

// RegisterFailure records a failed deploy attempt and applies the retry policy.
// When the failure is transient and the attempt budget is not yet exhausted it
// reschedules (bumps retryCount, pushes nextAttemptAt) and returns terminal=false.
// Otherwise it marks the Deployment failed and returns terminal=true.
func (d *Deployment) RegisterFailure(now time.Time, permanent bool, reason string, backoff BackoffPolicy) (terminal bool) {
	msg := reason
	d.errorMessage = &msg
	if !permanent && d.retryCount+1 < d.maxRetries {
		d.nextAttemptAt = now.Add(backoff.delay(d.retryCount))
		d.retryCount++
		return false
	}
	d.status = StatusFailed
	return true
}

// RecordOutcome attaches the terminal outcome to a previously dispatched
// (status=deployed) validation, seed-build, OR compile deployment — all three
// legs report a per-node terminal status the same way (validation.node.completed:v1 /
// seed.build.node.completed:v1 / compile.node.completed:v1). Production
// deployments announce their result through a different path and are rejected.
// Only "ok" and "failed" are accepted.
func (d *Deployment) RecordOutcome(outcome, logURI, runResultsURI string, now time.Time) error {
	if d.mode != ModeValidation && d.mode != ModeSeedBuild && d.mode != ModeCompile {
		return fmt.Errorf("RecordOutcome called on non-validation/seed-build/compile deployment %s", d.id)
	}
	if d.outcomeAt != nil {
		return fmt.Errorf("outcome already recorded for deployment %s", d.id)
	}
	if d.status != StatusDeployed {
		return fmt.Errorf("RecordOutcome from status %q; expected deployed", d.status)
	}
	if outcome != "ok" && outcome != "failed" {
		return fmt.Errorf("invalid outcome %q", outcome)
	}
	d.outcome = outcome
	d.dbtLogURI = logURI
	d.dbtRunResultsURI = runResultsURI
	ts := now
	d.outcomeAt = &ts
	return nil
}

// FailValidation drives a validation deployment to a terminal failed state and
// records outcome="failed" in one step. Unlike RecordOutcome it does not require
// a prior StatusDeployed: a validation row that fails BEFORE it is dispatched
// (not deployable, or a permanent pre-deploy deployer error) is still pending,
// yet must reach a terminal "failed" outcome so the per-release aggregate can be
// emitted. It is validation-only and idempotent-safe in that it rejects a second
// recording once an outcome exists.
func (d *Deployment) FailValidation(reason string, now time.Time) error {
	if d.mode != ModeValidation {
		return fmt.Errorf("FailValidation called on non-validation deployment %s", d.id)
	}
	if d.outcomeAt != nil {
		return fmt.Errorf("outcome already recorded for deployment %s", d.id)
	}
	msg := reason
	d.errorMessage = &msg
	d.status = StatusFailed
	d.outcome = "failed"
	ts := now
	d.outcomeAt = &ts
	return nil
}

// FailSeedBuild drives a seed-build deployment to a terminal failed state and
// records outcome="failed" in one step. It is the seed-build equivalent of
// FailValidation: a seed-build row that fails BEFORE it is dispatched (not
// deployable, or a permanent pre-deploy deployer error) is still pending yet
// must reach a terminal "failed" outcome so the per-release seed-build
// aggregate can be emitted.
func (d *Deployment) FailSeedBuild(reason string, now time.Time) error {
	if d.mode != ModeSeedBuild {
		return fmt.Errorf("FailSeedBuild called on non-seed-build deployment %s", d.id)
	}
	if d.outcomeAt != nil {
		return fmt.Errorf("outcome already recorded for deployment %s", d.id)
	}
	msg := reason
	d.errorMessage = &msg
	d.status = StatusFailed
	d.outcome = "failed"
	ts := now
	d.outcomeAt = &ts
	return nil
}

// FailCompile drives a compile deployment to a terminal failed state and
// records outcome="failed" in one step. It is the compile equivalent of
// FailSeedBuild: a compile row that fails BEFORE it is dispatched (not
// deployable, or a permanent pre-deploy deployer error) is still pending yet
// must reach a terminal "failed" outcome so the per-release compile aggregate
// can be emitted.
func (d *Deployment) FailCompile(reason string, now time.Time) error {
	if d.mode != ModeCompile {
		return fmt.Errorf("FailCompile called on non-compile deployment %s", d.id)
	}
	if d.outcomeAt != nil {
		return fmt.Errorf("outcome already recorded for deployment %s", d.id)
	}
	msg := reason
	d.errorMessage = &msg
	d.status = StatusFailed
	d.outcome = "failed"
	ts := now
	d.outcomeAt = &ts
	return nil
}

// Unblock transitions a gated validation deployment from blocked to pending so
// the dispatcher can pick it up. Caller decides readiness (all in-set upstreams
// succeeded); the aggregate only guards the source state.
func (d *Deployment) Unblock(now time.Time) error {
	if d.mode != ModeValidation {
		return fmt.Errorf("Unblock called on non-validation deployment %s", d.id)
	}
	if d.status != StatusBlocked {
		return fmt.Errorf("cannot Unblock from status %q", d.status)
	}
	d.status = StatusPending
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
	if d.status != StatusBlocked {
		return fmt.Errorf("cannot Skip from status %q", d.status)
	}
	msg := reason
	d.errorMessage = &msg
	d.status = StatusSkipped
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
func (d *Deployment) OutcomeAt() *time.Time    { return d.outcomeAt }
