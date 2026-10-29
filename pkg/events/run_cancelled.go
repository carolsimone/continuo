package events

import "time"

// RunCancelledSchemaVersion is the schema_version of run.cancelled:v1 entries.
const RunCancelledSchemaVersion = 1

// RunCancelled is the payload of a run.cancelled:v1 entry: run-controller cancelled
// the run, and every attempt of it that was dispatched and has not ended must
// be stopped. CancelledBy and Reason are the operator's own words and only
// displayed. Attempts lists each attempt to stop with the name of its Job; it
// is an empty array when none was in flight.
type RunCancelled struct {
	RunID       string             `json:"run_id"`
	CancelledBy string             `json:"cancelled_by"`
	Reason      string             `json:"reason"`
	Attempts    []CancelledAttempt `json:"attempts"`
}

// CancelledAttempt names one attempt of a cancelled run and the Job that runs
// it.
type CancelledAttempt struct {
	TaskID  string `json:"task_id"`
	Attempt int32  `json:"attempt"`
	JobName string `json:"job_name"`
}

// RunCancelledFields renders p as the Redis fields of one run.cancelled:v1
// entry. eventID is the id of the outbox row that carries the entry.
func RunCancelledFields(eventID, tenantID, producer string, occurredAt time.Time, p RunCancelled) (map[string]any, error) {
	if p.Attempts == nil {
		p.Attempts = []CancelledAttempt{}
	}
	env := Envelope{
		EventID:       eventID,
		TenantID:      tenantID,
		OccurredAt:    occurredAt,
		Producer:      producer,
		SchemaVersion: RunCancelledSchemaVersion,
	}
	return env.Fields(p)
}
