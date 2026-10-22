package events

import (
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
)

// taskDispatchWire reads a task.dispatch:v1 payload with the attempt as a
// pointer, so a payload without one is told apart from attempt 0.
type taskDispatchWire struct {
	TaskDispatch
	Attempt *int32 `json:"attempt"`
}

// DecodeTaskDispatch reads one task.dispatch:v1 entry. Every error wraps
// ErrMalformedEnvelope: an unreadable envelope, another schema_version, a
// payload that is not JSON, a run_id or task_id that is not a UUID, or an
// attempt that is missing, not an integer or negative. Those identify the
// attempt; the other fields are returned as written, for the consumer to judge.
func DecodeTaskDispatch(fields map[string]string) (Envelope, TaskDispatch, error) {
	env, payload, err := ParseEnvelope(fields)
	if err != nil {
		return Envelope{}, TaskDispatch{}, err
	}
	if env.SchemaVersion != TaskDispatchSchemaVersion {
		return Envelope{}, TaskDispatch{}, fmt.Errorf("%w: schema_version %d", ErrMalformedEnvelope, env.SchemaVersion)
	}
	var w taskDispatchWire
	if err := json.Unmarshal(payload, &w); err != nil {
		return Envelope{}, TaskDispatch{}, fmt.Errorf("%w: payload: %v", ErrMalformedEnvelope, err)
	}
	if err := validAttemptIdentity(w.RunID, w.TaskID, w.Attempt); err != nil {
		return Envelope{}, TaskDispatch{}, err
	}
	p := w.TaskDispatch
	p.Attempt = *w.Attempt
	return env, p, nil
}

// validAttemptIdentity checks the fields that name an attempt on both task
// streams: a run id and a task id that are UUIDs, and an attempt number of 0
// or more.
func validAttemptIdentity(runID, taskID string, attempt *int32) error {
	if _, err := uuid.Parse(runID); err != nil {
		return fmt.Errorf("%w: run_id %q", ErrMalformedEnvelope, runID)
	}
	if _, err := uuid.Parse(taskID); err != nil {
		return fmt.Errorf("%w: task_id %q", ErrMalformedEnvelope, taskID)
	}
	if attempt == nil {
		return fmt.Errorf("%w: no attempt for task %s", ErrMalformedEnvelope, taskID)
	}
	if *attempt < 0 {
		return fmt.Errorf("%w: attempt %d for task %s", ErrMalformedEnvelope, *attempt, taskID)
	}
	return nil
}
