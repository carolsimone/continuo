package model

// InitializeRunInput carries the handler-input data derived from an
// initialize.run:v1 stream message. The consumer hand-builds it from
// scalar Redis fields; the struct is not deserialised as a unit.
type InitializeRunInput struct {
	ScheduleName string
	RunID        string
	RerunTarget  *RerunTarget
}

// RerunTarget specifies the node to rerun from. It is a sub-DTO of
// InitializeRunInput, populated when the initialize.run:v1 message
// carries rerun_* fields.
type RerunTarget struct {
	ServiceName string
	SchemaName  string
	TableName   string
}
