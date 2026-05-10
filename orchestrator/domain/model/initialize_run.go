package model

// InitializeRunInput carries the handler-input data derived from an
// initialize.run:v1 stream message. The consumer hand-builds it from
// scalar Redis fields; the struct is not deserialised as a unit.
//
// The struct is also json.Marshal'd into message_processing.payload for
// forensic audit reads, so JSON tags use snake_case to keep that audit
// column shape stable across refactors. Do not strip the tags as "unused".
type InitializeRunInput struct {
	ScheduleName string       `json:"schedule_name"`
	RunID        string       `json:"run_id"`
	RerunTarget  *RerunTarget `json:"rerun_target,omitempty"`
}

// RerunTarget specifies the node to rerun from. It is a sub-DTO of
// InitializeRunInput, populated when the initialize.run:v1 message
// carries rerun_* fields.
//
// JSON tags exist because this struct is embedded in InitializeRunInput
// when that input is marshalled into message_processing.payload for
// forensic audit reads. Keep snake_case to preserve audit column shape.
type RerunTarget struct {
	ServiceName string `json:"service_name"`
	SchemaName  string `json:"schema_name"`
	TableName   string `json:"table_name"`
}
