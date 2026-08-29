// Package event defines the remediation.proposed:v1 payload and deterministic
// identifiers used for outbox dedup.
package event

import "github.com/google/uuid"

const EventType = "remediation_proposed"

var remediationProposedNamespace = uuid.MustParse("d2a7f3c1-8e94-4b6a-9c2d-3f5b7a1e0d8c")

// RemediationEventID maps (releaseID, attempt) to a stable UUID so a
// redelivered trigger does not produce a distinct downstream event.
func RemediationEventID(releaseID string, attempt int) uuid.UUID {
	return uuid.NewSHA1(remediationProposedNamespace, []byte(releaseID+"|"+itoa(attempt)))
}

// AggregateIDForRelease maps a release id to a stable outbox AggregateID.
func AggregateIDForRelease(releaseID string) uuid.UUID {
	return uuid.NewSHA1(remediationProposedNamespace, []byte("release:"+releaseID))
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}

// ProposedEdit is one proposed file change carried on the wire: its
// repository-relative path, the S3 pointers to the proposed content and
// unified diff, and the node whose source it changes.
type ProposedEdit struct {
	Path         string `json:"path"`
	ContentURI   string `json:"content_uri"`
	DiffURI      string `json:"diff_uri"`
	TargetNodeID string `json:"target_node_id"`
}

// RemediationProposed is the pointer-only trigger output: it carries S3 pointers
// to the proposed SQL + diff and the model's short rationale (no warehouse data).
type RemediationProposed struct {
	EventID   string `json:"event_id"`
	Source    string `json:"source"`
	ReleaseID string `json:"release_id"`
	// RemediationRound is the release's remediation round this attempt belongs to.
	RemediationRound int    `json:"remediation_round"`
	NodeID           string `json:"node_id"`
	// ResolvedNodeIDs is the failing nodes this attempt addresses, sorted.
	ResolvedNodeIDs []string `json:"resolved_node_ids"`
	ErrorSignature  string   `json:"error_signature"`
	ProposedSQLURI  string   `json:"proposed_sql_uri"`
	DiffURI         string   `json:"diff_uri"`
	// Edits is the full multi-file description of this attempt's proposed
	// changes.
	Edits                  []ProposedEdit `json:"edits"`
	Rationale              string         `json:"rationale"`
	Confidence             string         `json:"confidence"`
	SuspectedRootCauseNode string         `json:"suspected_root_cause_node,omitempty"`
	Model                  string         `json:"model"`
	Attempt                int            `json:"attempt"`
	SourceResolved         bool           `json:"source_resolved"`
	ProposedAt             string         `json:"proposed_at"`
}
