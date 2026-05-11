package model

// InitializeRunInput carries the handler-input data derived from an
// initialize.run:v1 stream message. The consumer hand-builds it from
// scalar Redis fields; the struct is not deserialised as a unit.
//
// The struct is also json.Marshal'd into message_processing.payload for
// forensic audit reads, so JSON tags use snake_case to keep that audit
// column shape stable across refactors. Do not strip the tags as "unused".
type InitializeRunInput struct {
	ScheduleName string `json:"schedule_name"`
	RunID        string `json:"run_id"`
}
