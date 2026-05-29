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
	StatusDeployed Status = "deployed"
	StatusFailed   Status = "failed"
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
	outcome   string
	dbtLogURI string
	outcomeAt *time.Time
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

// NewValidationDeployment starts a fresh pending validation Deployment due
// immediately. It carries the ValidationDeployTask identity instead of a
// production DeployTask; the dispatcher branches on Mode.
func NewValidationDeployment(cmd command.ValidationDeployTask, msgProcID *uuid.UUID, now time.Time) *Deployment {
	return &Deployment{
		id:                  uuid.New(),
		messageProcessingID: msgProcID,
		mode:                ModeValidation,
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
	outcome, dbtLogURI string,
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
		outcomeAt:           outcomeAt,
	}
}

// IsDeployable reports whether the command carries the identity and target a
// deploy needs. A row whose job_params could not be deserialized recovers only
// its task/schedule identity, so this returns false and the dispatcher fails it
// permanently rather than attempting a meaningless deploy.
func (d *Deployment) IsDeployable() bool {
	if d.mode == ModeValidation {
		return d.validationCmd.JobName != "" &&
			d.validationCmd.NodeID != "" &&
			d.validationCmd.ReleaseID != "" &&
			d.validationCmd.NodeType != "" &&
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

// RecordOutcome attaches the terminal validation outcome to a previously
// dispatched (status=deployed) validation deployment. It is validation-only:
// production deployments announce their result through a different path. Only
// "ok" and "failed" are accepted outcomes.
func (d *Deployment) RecordOutcome(outcome, logURI string, now time.Time) error {
	if d.mode != ModeValidation {
		return fmt.Errorf("RecordOutcome called on non-validation deployment %s", d.id)
	}
	if d.status != StatusDeployed {
		return fmt.Errorf("RecordOutcome from status %q; expected deployed", d.status)
	}
	if outcome != "ok" && outcome != "failed" {
		return fmt.Errorf("invalid outcome %q", outcome)
	}
	d.outcome = outcome
	d.dbtLogURI = logURI
	ts := now
	d.outcomeAt = &ts
	return nil
}

// Accessors used by adapters (persistence) and the application service.
func (d *Deployment) ID() uuid.UUID                   { return d.id }
func (d *Deployment) MessageProcessingID() *uuid.UUID { return d.messageProcessingID }
func (d *Deployment) Mode() Mode                      { return d.mode }
func (d *Deployment) Command() command.DeployTask     { return d.command }
func (d *Deployment) ValidationCommand() command.ValidationDeployTask {
	return d.validationCmd
}
func (d *Deployment) ReleaseID() string        { return d.validationCmd.ReleaseID }
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
func (d *Deployment) OutcomeAt() *time.Time    { return d.outcomeAt }
