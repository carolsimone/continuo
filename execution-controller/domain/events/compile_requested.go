// execution-controller/domain/events/compile_requested.go
package events

import "github.com/google/uuid"

// CompileRequested is the parsed compile.requested:v1 stream payload — a typed,
// in-process representation of the compile request emitted by release-controller.
// execution-controller enqueues exactly one compile Deployment for the named project.
//
// OutboxEntryID is the release-controller outbox row ID, carried through as
// provenance. Zero value (uuid.Nil) means the inbound message did not carry
// the field.
type CompileRequested struct {
	OutboxEntryID uuid.UUID
	ReleaseID     string
	Project       string
	ImageTag      string
	Bucket        string
	// CandidateSchema is the release's candidate schema, echoed by
	// release-controller for the parse-export leg of the compile Job. Absent on
	// the wire (older messages) parses to empty, and the compile Job runs
	// without parse-export containers.
	CandidateSchema string
	// SourceOverlayURI locates the source-overlay tarball a fix's
	// release check's compile Job lays over the project before compiling; empty for
	// every production release.
	SourceOverlayURI string
}
