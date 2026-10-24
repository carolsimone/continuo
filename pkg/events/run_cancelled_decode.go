package events

import (
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
)

// runCancelledWire reads a run.cancelled:v1 payload with each attempt number
// as a pointer, so an attempt without one is told apart from attempt 0.
type runCancelledWire struct {
	RunCancelled
	Attempts []cancelledAttemptWire `json:"attempts"`
}

type cancelledAttemptWire struct {
	CancelledAttempt
	Attempt *int32 `json:"attempt"`
}

// DecodeRunCancelled reads one run.cancelled:v1 entry. Every error wraps
// ErrMalformedEnvelope: an unreadable envelope, another schema_version, a
// payload that is not JSON, a run_id or task_id that is not a UUID, an attempt
// that is missing, not an integer or negative, or a job_name that is empty.
// Attempts is never nil: a payload without any reads as no attempt in flight.
func DecodeRunCancelled(fields map[string]string) (Envelope, RunCancelled, error) {
	env, payload, err := ParseEnvelope(fields)
	if err != nil {
		return Envelope{}, RunCancelled{}, err
	}
	if env.SchemaVersion != RunCancelledSchemaVersion {
		return Envelope{}, RunCancelled{}, fmt.Errorf("%w: schema_version %d", ErrMalformedEnvelope, env.SchemaVersion)
	}
	var w runCancelledWire
	if err := json.Unmarshal(payload, &w); err != nil {
		return Envelope{}, RunCancelled{}, fmt.Errorf("%w: payload: %v", ErrMalformedEnvelope, err)
	}
	if _, err := uuid.Parse(w.RunID); err != nil {
		return Envelope{}, RunCancelled{}, fmt.Errorf("%w: run_id %q", ErrMalformedEnvelope, w.RunID)
	}
	p := w.RunCancelled
	p.Attempts = make([]CancelledAttempt, 0, len(w.Attempts))
	for _, a := range w.Attempts {
		if err := validAttemptIdentity(w.RunID, a.TaskID, a.Attempt); err != nil {
			return Envelope{}, RunCancelled{}, err
		}
		if a.JobName == "" {
			return Envelope{}, RunCancelled{}, fmt.Errorf("%w: no job_name for task %s attempt %d", ErrMalformedEnvelope, a.TaskID, *a.Attempt)
		}
		p.Attempts = append(p.Attempts, CancelledAttempt{TaskID: a.TaskID, Attempt: *a.Attempt, JobName: a.JobName})
	}
	return env, p, nil
}
