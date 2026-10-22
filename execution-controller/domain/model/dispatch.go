package model

import (
	"fmt"
	"time"

	"github.com/carolsimone/continuo/execution-controller/domain/command"
	pkgmodel "github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/google/uuid"
)

// The statuses a dispatch's known_status can name.
const (
	KnownDispatched = "dispatched"
	KnownQueued     = "queued"
	KnownRunning    = "running"
)

// DispatchAction is what execution-controller does with one dispatch of an
// attempt.
type DispatchAction int

const (
	// ResendReport: the attempt is accepted already; its last report is sent
	// again.
	ResendReport DispatchAction = iota + 1
	// IgnoreOlder: a later attempt of the task is accepted, so this one is
	// never created.
	IgnoreOlder
	// RefuseInvalid: the dispatch cannot run; the attempt is recorded failed
	// with the problem's reason.
	RefuseInvalid
	// AdoptJob: no record of the attempt is held, but its Job exists; the
	// record is rebuilt from the dispatch and the Job.
	AdoptJob
	// CreateAttempt: the dispatch is new; the attempt is queued for its Job.
	CreateAttempt
	// ReportLost: state knows the attempt as acknowledged, yet neither its
	// record nor its Job is held; it is reported failed as attempt_lost.
	ReportLost
)

// DecideDispatch decides what a dispatch leads to, from whether the attempt's
// record is held, whether a later attempt's is, the problem (if any) that
// stops the dispatch from running, whether a Job carrying the task and
// attempt exists, and state's known status. Only a dispatch state still
// knows as dispatched creates an attempt: an acknowledged attempt is never
// run again because of a question.
func DecideDispatch(held, laterHeld bool, problem pkgmodel.FailureReason, jobOfAttempt bool, knownStatus string) DispatchAction {
	switch {
	case held:
		return ResendReport
	case laterHeld:
		return IgnoreOlder
	case problem != "":
		return RefuseInvalid
	case jobOfAttempt:
		return AdoptJob
	case knownStatus == KnownDispatched:
		return CreateAttempt
	}
	return ReportLost
}

// DispatchProblem returns the reason and message of what stops cmd, the
// command of a dispatch state knows in knownStatus, from running; an empty
// reason when nothing does. The Job itself may still be refused when it is
// built (deploy.ErrMalformedDispatch).
func DispatchProblem(cmd command.DeployTask, knownStatus string) (pkgmodel.FailureReason, string) {
	if _, err := pkgmodel.ParseNodeType(cmd.NodeType); err != nil {
		return pkgmodel.FailureReasonInvalidNodeType, err.Error()
	}
	problem := ""
	switch {
	case knownStatus != KnownDispatched && knownStatus != KnownQueued && knownStatus != KnownRunning:
		problem = fmt.Sprintf("known_status %q is not dispatched, queued or running", knownStatus)
	case cmd.JobName == "" || len(cmd.JobName) > 63:
		problem = fmt.Sprintf("job_name %q is not a Kubernetes name of 1 to 63 characters", cmd.JobName)
	case cmd.ImageTag == "":
		problem = "image_tag is empty"
	case cmd.TimeoutSeconds < 1:
		problem = fmt.Sprintf("timeout_seconds %d is below 1", cmd.TimeoutSeconds)
	default:
		if _, err := pkgmodel.ParseOperation(cmd.Operation); err != nil {
			problem = err.Error()
		}
	}
	if problem != "" {
		return pkgmodel.FailureReasonMalformedDispatch, problem
	}
	return "", ""
}

// NewAdoptedDeployment rebuilds the record of an attempt whose Job exists:
// it holds a slot and is starting, as if just created, so the observer
// follows the Job to its end.
func NewAdoptedDeployment(cmd command.DeployTask, msgProcID *uuid.UUID, now time.Time) *Deployment {
	d := NewDeployment(cmd, msgProcID, now)
	d.status = StatusStarting
	d.deployedAt = &now
	return d
}

// NewRefusedDeployment records an attempt that cannot run as dispatched:
// failed at once with reason and message, its outcome recorded, so a repeated
// dispatch finds it and is answered with its report.
func NewRefusedDeployment(cmd command.DeployTask, msgProcID *uuid.UUID, reason pkgmodel.FailureReason, message string, now time.Time) *Deployment {
	d := NewDeployment(cmd, msgProcID, now)
	d.status = StatusFailed
	d.setError(message)
	d.outcome = "failed"
	d.outcomeAt = &now
	d.facts, _ = d.facts.Merge(AttemptFacts{Reason: reason, Message: message})
	return d
}
