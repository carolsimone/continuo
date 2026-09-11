package ports

import (
	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
)

// RejectionShape selects which release.rejected:v1 body a rejection renders
// as. Each leg that can end a candidate carries different evidence — the
// parse leg has the parser's own diagnostic and no log, the compile leg has
// logs and no source location, the duplicate-table check has two competing
// claimants and no stage at all — so the event has one body per shape rather
// than one union body with most keys empty.
type RejectionShape string

const (
	// RejectionShapeParse is the body emitted when the candidate's manifests
	// fail to parse. Its stage is "parse".
	RejectionShapeParse RejectionShape = "parse"
	// RejectionShapeCompile is the body emitted when the dbt compile job
	// fails. Its stage is "compile".
	RejectionShapeCompile RejectionShape = "compile"
	// RejectionShapeSeedBuild is the body emitted when the candidate seed
	// build fails. Its stage is "seed_build" and it additionally names the
	// candidate schema the seeds were being built into.
	RejectionShapeSeedBuild RejectionShape = "seed_build"
	// RejectionShapeValidation is the body emitted when the validation run
	// fails. Its stage is "validation"; it carries the aggregate status and
	// each failing node's changed ancestors instead of an error detail.
	RejectionShapeValidation RejectionShape = "validation"
	// RejectionShapeDuplicateTable is the body emitted when two nodes claim
	// the same warehouse relation or the same unique_id. The check runs
	// between legs rather than inside one, so the body carries no stage.
	RejectionShapeDuplicateTable RejectionShape = "duplicate_table"
	// RejectionShapeUnbuildableUpstream is the body emitted when a changed
	// node references an upstream absent from the candidate topology. It
	// names only the release, the reason and the offending edges: there is no
	// per-node evidence a fixer could act on.
	RejectionShapeUnbuildableUpstream RejectionShape = "unbuildable_cross_service_upstream"
)

// ChangedAncestor is one changed upstream of a failing validation node,
// carrying the location THIS candidate declares for it: a fix must edit the
// file the candidate holds the ancestor in, which for a node renamed or moved
// in this release is not where the promoted graph would place it.
type ChangedAncestor struct {
	NodeID   string
	FilePath string
	Service  string
	Depth    int
}

// RejectedNode is the union of the per-node evidence the legs produce. A
// given shape populates only the fields its leg can know: the encoder renders
// that shape's subset and ignores the rest, so a handler never has to reason
// about which keys reach the wire.
type RejectedNode struct {
	NodeID string
	Status string

	// Kind and Detail carry the parse leg's failure classification and the
	// parser's own diagnostic — the parse leg produces no log to fetch.
	Kind   string
	Detail string

	// FilePath, Service and NodeType locate the node's source. The parse,
	// seed-build, validation and duplicate-table legs resolve them from the
	// candidate topology; the compile leg does not carry them.
	FilePath string
	Service  string
	NodeType string

	// DBTLogURI and RunResultsURI point at the artifacts of the job that
	// produced the failure.
	DBTLogURI     string
	RunResultsURI string

	// CandidateArtifactURI points at the node's compiled candidate SQL.
	CandidateArtifactURI string

	// RelationID, OtherService and OtherFilePath describe a duplicate-table
	// collision: the contested relation, and the competing claimant that a
	// rename must move away from.
	RelationID    string
	OtherService  string
	OtherFilePath string

	// ChangedAncestors lists the changed upstreams that may be the root cause
	// of a failing validation node.
	ChangedAncestors []ChangedAncestor
}

// ReleaseRejection is the tag-free description of a candidate that is ending
// rejected. Handlers fill in the values their leg knows and name the shape;
// the encoder owns the wire keys.
type ReleaseRejection struct {
	Shape     RejectionShape
	ReleaseID string
	Reason    pkg_model.RejectReason

	// ErrorDetail is the operator-facing explanation of the failure. The
	// validation shape carries none: its evidence is per node.
	ErrorDetail string

	// AggregateStatus is the validation run's own terminal status.
	AggregateStatus string

	// FailingNodes names every node that failed, and PerNode carries the
	// evidence for the nodes a fixer can act on. A nil slice and an empty one
	// are distinct on the wire (null vs []), so both are passed through as
	// the handler built them.
	FailingNodes []string
	PerNode      []RejectedNode

	// Repo and CommitSHA identify the source the changed service ships from,
	// and CodeBundleURI the packaged copy of it, so a consumer can read the
	// files the failing nodes are declared in.
	Repo          string
	CommitSHA     string
	CodeBundleURI string

	// CandidateSchema names the schema the seed build was writing into.
	CandidateSchema string
}

// ReleaseRejectedEncoder renders the release.rejected:v1 payload for a run
// that is ending rejected. It owns the wire keys so handlers pass values.
// Implemented by an adapter; a shape the implementation does not know is an
// error rather than a silently truncated body.
type ReleaseRejectedEncoder interface {
	Encode(rej ReleaseRejection) ([]byte, error)
}
