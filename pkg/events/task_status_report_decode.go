package events

import (
	"encoding/json"
	"fmt"

	"github.com/carolsimone/continuo/pkg/domain/model"
)

// taskStatusReportWire reads a task.status.updated:v2 payload with the
// attempt as a pointer, so a payload without one is told apart from attempt 0.
type taskStatusReportWire struct {
	TaskStatusReport
	Attempt *int32 `json:"attempt"`
}

// DecodeTaskStatusReport reads one task.status.updated:v2 entry. Every error
// wraps ErrMalformedEnvelope: an unreadable envelope, another schema_version,
// a payload that is not JSON, a run_id or task_id that is not a UUID, an
// attempt that is missing, not an integer or negative, a status other than
// queued, running, succeeded, failed or stopped, a failed report without a
// declared failure_reason or whose failure_class is not that reason's class,
// and any other report, a stopped one included, that names a class or reason.
func DecodeTaskStatusReport(fields map[string]string) (Envelope, TaskStatusReport, error) {
	env, payload, err := ParseEnvelope(fields)
	if err != nil {
		return Envelope{}, TaskStatusReport{}, err
	}
	if env.SchemaVersion != TaskStatusReportSchemaVersion {
		return Envelope{}, TaskStatusReport{}, fmt.Errorf("%w: schema_version %d", ErrMalformedEnvelope, env.SchemaVersion)
	}
	var w taskStatusReportWire
	if err := json.Unmarshal(payload, &w); err != nil {
		return Envelope{}, TaskStatusReport{}, fmt.Errorf("%w: payload: %v", ErrMalformedEnvelope, err)
	}
	if err := validAttemptIdentity(w.RunID, w.TaskID, w.Attempt); err != nil {
		return Envelope{}, TaskStatusReport{}, err
	}
	p := w.TaskStatusReport
	p.Attempt = *w.Attempt
	if err := validReportOutcome(p); err != nil {
		return Envelope{}, TaskStatusReport{}, err
	}
	return env, p, nil
}

// validReportOutcome checks a report's status, a declared attempt_status
// other than dispatched (which only state sends), and, for a failure, that its
// reason is declared and its class is the reason's.
func validReportOutcome(p TaskStatusReport) error {
	if !p.Status.IsValid() || p.Status == model.AttemptStatusDispatched {
		return fmt.Errorf("%w: status %q of task %s", ErrMalformedEnvelope, p.Status, p.TaskID)
	}
	if p.Status != model.AttemptStatusFailed {
		if p.FailureClass != "" || p.FailureReason != "" {
			return fmt.Errorf("%w: a %s report of task %s names a failure", ErrMalformedEnvelope, p.Status, p.TaskID)
		}
		return nil
	}
	reason := model.FailureReason(p.FailureReason)
	if !reason.IsValid() {
		return fmt.Errorf("%w: failure_reason %q of task %s", ErrMalformedEnvelope, p.FailureReason, p.TaskID)
	}
	if class := reason.Class(); string(class) != p.FailureClass {
		return fmt.Errorf("%w: failure_class %q of task %s, but %s is a %s failure",
			ErrMalformedEnvelope, p.FailureClass, p.TaskID, reason, class)
	}
	return nil
}
