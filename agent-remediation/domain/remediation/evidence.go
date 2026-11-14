package remediation

import "slices"

// EvidenceKey names one piece of evidence a cluster's steps share.
type EvidenceKey string

// The evidence keys, each with the Go type its value carries.
const (
	// KeyFailure holds a Failure.
	KeyFailure EvidenceKey = "failure"
	// KeyDraft holds a Draft.
	KeyDraft EvidenceKey = "draft"
	// KeySourceFiles holds SourceFiles.
	KeySourceFiles EvidenceKey = "source_files"
	// KeyDBTLog holds Text.
	KeyDBTLog EvidenceKey = "dbt_log"
	// KeyFailurePrecedents holds []prompt.Precedent.
	KeyFailurePrecedents EvidenceKey = "failure_precedents"
	// KeyUpstreamChanges holds []prompt.UpstreamChange.
	KeyUpstreamChanges EvidenceKey = "upstream_changes"
	// KeyCandidateSQL holds Text.
	KeyCandidateSQL EvidenceKey = "candidate_sql"
	// KeyCodeBundleSource holds CandidateSource.
	KeyCodeBundleSource EvidenceKey = "code_bundle_source"
	// KeyOwnChangeDiff holds Text.
	KeyOwnChangeDiff EvidenceKey = "own_change_diff"
	// KeyChangedAncestorDiffs holds []prompt.ReleaseUpstreamChange.
	KeyChangedAncestorDiffs EvidenceKey = "changed_ancestor_diffs"
	// KeyContractFile holds ContractFile.
	KeyContractFile EvidenceKey = "contract_file"
	// KeyContractEntry holds Text.
	KeyContractEntry EvidenceKey = "contract_entry"
	// KeyPriorAttempts holds []prompt.PriorAttempt.
	KeyPriorAttempts EvidenceKey = "prior_attempts"
	// KeyUpstreamTarget holds UpstreamTarget.
	KeyUpstreamTarget EvidenceKey = "upstream_target"
	// KeyPackagedContract holds Bytes.
	KeyPackagedContract EvidenceKey = "packaged_contract"
)

// Text is a block of text evidence.
type Text string

// Bytes is a block of binary evidence.
type Bytes []byte

// SourceFiles is the source of the failing node's service: its files by name,
// the order to show them in, the primary file and the owning service.
type SourceFiles struct {
	Files   map[string]string
	Order   []string
	Primary string
	Service string
}

// CandidateSource is a node's source read from the release's code bundle, with
// where it came from.
type CandidateSource struct {
	Source   string
	Origin   string
	FilePath string
	Service  string
}

// ContractFile is the contract yaml that declares a python node.
type ContractFile struct {
	Schema      string
	Table       string
	RepoRoot    string
	ContractDir string
	YAMLPath    string
	YAMLText    string
}

// UpstreamTarget is the changed ancestor an upstream cluster's fix edits.
type UpstreamTarget struct {
	NodeID        string
	Source        string
	FilePath      string
	Service       string
	OwnChangeDiff string
}

// Evidence is the typed bag of facts the steps of one cluster gather and read.
// Its zero value is an empty bag. It is immutable: With returns a copy.
type Evidence struct {
	values map[EvidenceKey]any
}

// NewEvidence returns an empty Evidence.
func NewEvidence() Evidence {
	return Evidence{values: map[EvidenceKey]any{}}
}

// With returns a copy of e that also holds v under k, replacing any value
// already there.
func (e Evidence) With(k EvidenceKey, v any) Evidence {
	next := make(map[EvidenceKey]any, len(e.values)+1)
	for key, val := range e.values {
		next[key] = val
	}
	next[k] = v
	return Evidence{values: next}
}

// Get returns the value under k as a T. It reports false when k is absent or
// holds another type.
func Get[T any](e Evidence, k EvidenceKey) (T, bool) {
	v, ok := e.values[k].(T)
	return v, ok
}

// Keys returns the keys e holds, sorted.
func (e Evidence) Keys() []EvidenceKey {
	keys := make([]EvidenceKey, 0, len(e.values))
	for k := range e.values {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}
