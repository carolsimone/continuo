// Package failure holds the event-agnostic domain model for classifying a
// failed dbt node: the evidence gathered about a failure, the category it is
// sorted into, and the decision (emit a failure a fix can reach, or drop one
// no fix can reach, which the trigger still carries, marked unfixable).
package failure

import "github.com/carolsimone/continuo/pkg/domain/model"

// Category is the deterministic classification of a failed node.
type Category string

const (
	CategoryLogic          Category = "logic"
	CategoryTest           Category = "test"
	CategoryUnknown        Category = "unknown"
	CategoryInfraTransient Category = "infra_transient"
)

// Decision is the routing outcome: emit a failure a fix proposal can reach, or
// drop one no change to the release's source fixes. A dropped failure is
// recorded and forwarded on the trigger, marked unfixable.
type Decision string

const (
	DecisionEmit Decision = "emit"
	DecisionDrop Decision = "drop"
)

// FailureEvidence is the event-agnostic input to the classifier. Ingress
// adapters translate a source event (e.g. release.rejected:v1) into this
// value object; the classifier never sees the originating event.
type FailureEvidence struct {
	// Source is the pipeline stage that produced the failure. Validation-time
	// (blue/green) failures are classified; production-run failures enter
	// through a separate ingress adapter.
	Source    model.RemediationSource
	ReleaseID string
	// RemediationRound is the release's remediation round this evidence
	// belongs to: 1 for the rejection itself, +1 per human "try again" on
	// the release. 0 and 1 both mean round 1 — ClassifyRejection normalises
	// an unset value before using it.
	RemediationRound     int
	NodeID               string
	RelationID           string // optional; the contested physical relation for a duplicate-relation failure — distinct from NodeID (the target claimant's own identity), which can name a different string once the target carries an alias. Empty for every other source.
	DBTLogURI            string
	RunResultsURI        string
	CandidateArtifactURI string
	Repo                 string
	CommitSHA            string
	FilePath             string // optional; offending source path — extracted from the dbt log for compile; threaded from the candidate topology for validation, seed_build, and duplicate_table
	Project              string // optional; owning dbt project for source resolution; set for validation, seed_build, and duplicate_table failures; empty for compile (NodeID is the project)
	NodeType             string // optional; the failing node's kind (dbt-model, dbt-seed, dbt-snapshot, python-node, python-csv, python-api); set for validation, duplicate_table and parse failures — this is what selects the agent's fixer lane for a python node without a topology lookup of its own; empty for compile and seed_build
	OtherProject         string // optional; for a duplicate-relation failure, the competing project that also produces the contested relation (RelationID)
	OtherFilePath        string // optional; source path of that competing node — the only discriminator when both claimants are in one project
	// CodeBundleURI locates the rejected release's code-bundle document;
	// empty when parse never completed. Forwarded onto the trigger event for
	// catalog's case base.
	CodeBundleURI string
	// ParseKind is the contract kind of a parse failure (invalid_sql or
	// unqualified_reference for a healable one); empty for every other source.
	ParseKind model.ParseFailureKind
	// Detail is the parser's own error text for a parse failure, carried on
	// the rejection because no log exists; empty for every other source.
	Detail string
	// ChangedAncestors are the node's changed transitive ancestors in the
	// rejected release, as release-controller stamped them, each with the
	// location THIS release's candidate topology declares for it; forwarded onto
	// the trigger so the agent can group failures by shared root cause and edit
	// the ancestor where the candidate actually holds it.
	ChangedAncestors []ChangedAncestor
}

// ChangedAncestor is one upstream node of a failing node whose content changed
// in the rejected release. The location travels with the id because an upstream
// fix has to edit the file the candidate declares: an ancestor renamed or moved
// in this release still sits at its old path in the promoted graph, so
// resolving the id there would edit a file that no longer holds the node. Depth
// is the ancestor's minimum upstream hop distance from the failing node (1 = a
// direct upstream), forwarded verbatim so a fixer can pick the nearest cause.
type ChangedAncestor struct {
	NodeID   string
	FilePath string
	Project  string
	Depth    int
}
