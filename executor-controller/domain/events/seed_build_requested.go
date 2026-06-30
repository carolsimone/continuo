// executor-controller/domain/events/seed_build_requested.go
package events

import (
	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/google/uuid"
)

// SeedBuildNode is one seed entry in a SeedBuildRequested.Seeds slice. Seeds
// are dbt roots and have no upstream dependencies or candidate SQL URI — they
// are built directly into the candidate schema via `dbt seed`.
type SeedBuildNode struct {
	NodeID      string // dbt unique_id
	ServiceName string
	SchemaName  string
	TableName   string
	NodeType    pkg_model.NodeType // always NodeTypeDbtSeed
	ImageTag    string
}

// SeedBuildRequested is the parsed seed.build.requested:v1 stream payload — a
// typed, in-process representation of a candidate-release seed-build request
// emitted by release-controller. The executor enqueues one seed-build
// deployment per seed in Seeds.
//
// OutboxEntryID is the release-controller outbox row ID, carried through as
// provenance. Zero value (uuid.Nil) means the inbound message did not carry
// the field.
//
// SeedIDsInOrder lists the same dbt unique_ids as Seeds[].NodeID. The parser
// guarantees the two agree as a set.
type SeedBuildRequested struct {
	OutboxEntryID  uuid.UUID
	ReleaseID      string
	Mode           string
	Seeds          []SeedBuildNode
	SeedIDsInOrder []string
	ImageTags      map[string]string
	CandidateSchema string
}
