package run

import (
	"strings"

	"github.com/google/uuid"
)

// NodeKey is a value object that uniquely identifies a table within the run.
type NodeKey struct {
	ServiceName string
	SchemaName  string
	TableName   string
}

// RunStatus is the lifecycle status of the Run aggregate root.
type RunStatus string

const (
	RunStatusInitialized RunStatus = "INITIALIZED"
	RunStatusInProgress  RunStatus = "IN_PROGRESS"
	RunStatusSucceeded   RunStatus = "SUCCEEDED"
	RunStatusFailed      RunStatus = "FAILED"
)

// terminalStatusCanonical maps every casing a writer can produce to the single
// canonical lowercase form persisted on :Run.terminal_status. The in-memory
// aggregate vocabulary is uppercase ("SUCCEEDED"/"FAILED"); the run.finalized:v1
// wire and the snapshot writer use lowercase ("succeeded"/"failed"/"cancelled").
// Normalizing here lets IsTerminal() and the write boundary agree on one form.
var terminalStatusCanonical = map[string]string{
	"succeeded": "succeeded",
	"failed":    "failed",
	"cancelled": "cancelled",
}

// IsTerminal reports whether the status is a terminal run outcome. It is
// case-insensitive so it holds for every value any writer stores, whether the
// uppercase in-memory enum or the lowercase wire/stored form a rehydrated
// aggregate carries.
func (s RunStatus) IsTerminal() bool {
	_, ok := terminalStatusCanonical[strings.ToLower(string(s))]
	return ok
}

// CanonicalTerminalStatus returns the canonical lowercase form that must be
// stamped on :Run.terminal_status, or "" when the status is not terminal. The
// write boundary uses the empty string as the "do not stamp" sentinel so a
// non-terminal aggregate never overwrites a terminal projection.
func (s RunStatus) CanonicalTerminalStatus() string {
	return terminalStatusCanonical[strings.ToLower(string(s))]
}

// RunNode is an entity within the Run aggregate, representing one table's
// execution slot in this run.
type RunNode struct {
	Key             NodeKey
	TaskID          uuid.UUID
	Status          string // domain.NodeStatus values: PENDING, RUNNING, SUCCEEDED, FAILED, SKIPPED
	ScheduleName    string
	NodeType        string
	ManifestVersion string
	ImageTag        string
	Upstreams       []NodeKey // used to check if all upstreams are terminal (unblocking)
	Downstreams     []NodeKey // immediate downstream keys (cascade skip traversal)
}

func (n *RunNode) isTerminal() bool {
	return n.Status == "SUCCEEDED" || n.Status == "FAILED" || n.Status == "SKIPPED"
}
