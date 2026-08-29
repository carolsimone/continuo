// Package failure holds the event-agnostic domain model for classifying a
// failed dbt node: the evidence gathered about a failure, the category it is
// sorted into, and the decision (emit a remediation trigger, or drop).
package failure

// Source identifies which pipeline produced the failure. Validation-time
// (blue/green) failures are classified; production-run failures enter through
// a separate ingress adapter.
type Source string

const (
	SourceValidation Source = "validation"
	SourceCompile    Source = "compile"
	SourceSeed       Source = "seed_build"
	// SourceDuplicateTable is a parse-time rejection: two nodes in the release's
	// topology claim the same relation. It runs before any Job, so evidence for
	// it carries no dbt log.
	SourceDuplicateTable Source = "duplicate_table"
)

// Category is the deterministic classification of a failed node.
type Category string

const (
	CategoryLogic          Category = "logic"
	CategoryTest           Category = "test"
	CategoryUnknown        Category = "unknown"
	CategoryInfraTransient Category = "infra_transient"
)

// Decision is the routing outcome: emit a remediation trigger or drop it.
type Decision string

const (
	DecisionEmit Decision = "emit"
	DecisionDrop Decision = "drop"
)

// FailureEvidence is the event-agnostic input to the classifier. Ingress
// adapters translate a source event (e.g. release.rejected:v1) into this
// value object; the classifier never sees the originating event.
type FailureEvidence struct {
	Source    Source
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
	Service              string // optional; owning dbt service for source resolution; set for validation, seed_build, and duplicate_table failures; empty for compile (NodeID is the service)
	NodeType             string // optional; the failing node's kind (dbt-model, dbt-seed, dbt-snapshot, python-model, python-csv); set for validation and duplicate-relation failures — this is what lets the agent skip a python node without a topology lookup of its own; empty for compile and seed_build
	OtherService         string // optional; for a duplicate-relation failure, the competing service that also produces the contested relation (RelationID)
	OtherFilePath        string // optional; source path of that competing node — the only discriminator when both claimants are in one service
	// CodeBundleURI locates the rejected release's code-bundle document;
	// empty when parse never completed. Forwarded onto the trigger event for
	// the orchestrator's case base.
	CodeBundleURI string
	// Shadow is true when the rejected release was a shadow release — one
	// posted by agent-remediation to verify a proposed fix, which never
	// promotes and never touches current_prod. A shadow rejection means the
	// proposed fix did not work; the classifier still records it (so the drop
	// is never invisible) but must not enqueue a remediation trigger for it,
	// or a failed fix attempt would trigger a remediation of itself.
	Shadow bool
	// ChangedAncestorIDs are the node's changed transitive ancestors in the
	// rejected release, as release-controller stamped them; forwarded onto the
	// trigger so the agent can group failures by shared root cause.
	ChangedAncestorIDs []string
}
