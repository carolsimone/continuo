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

// Healable reports whether a category warrants a remediation trigger. Only
// confidently-infrastructure failures are dropped; everything else — including
// the catch-all unknown bucket — flows to the agent (under-drop policy).
func (c Category) Healable() bool {
	return c != CategoryInfraTransient
}

// FailureEvidence is the event-agnostic input to the classifier. Ingress
// adapters translate a source event (e.g. release.rejected:v1) into this
// value object; the classifier never sees the originating event.
type FailureEvidence struct {
	Source               Source
	ReleaseID            string
	NodeID               string
	RelationID           string // optional; the contested physical relation for a duplicate-relation failure — distinct from NodeID (the target claimant's own identity), which can name a different string once the target carries an alias. Empty for every other source.
	DBTLogURI            string
	RunResultsURI        string
	CandidateArtifactURI string
	Repo                 string
	CommitSHA            string
	FilePath             string // optional; offending source path for compile (from dbt log) or seed_build (from candidate topology); empty → resolve via Ancestry(NodeID)
	Service              string // optional; owning dbt service for source resolution; set for seed_build failures; empty for compile (NodeID is the service)
	NodeType             string // optional; the target claimant's kind for a duplicate-relation failure (dbt-model, dbt-seed, dbt-snapshot, python-model); empty for every other source
	OtherService         string // optional; for a duplicate-relation failure, the competing service that also produces NodeID
	OtherFilePath        string // optional; source path of that competing node — the only discriminator when both claimants are in one service
}
