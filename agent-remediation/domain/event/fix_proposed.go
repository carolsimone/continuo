// Package event defines the fix.proposed:v1 payload and deterministic
// identifiers used for outbox dedup.
package event

import "github.com/google/uuid"

const EventType = "fix_proposed"

// AggregateType is the outbox aggregate type of every event this component
// emits. The relay withholds a row while an older open row shares its aggregate
// (type and id), so all of a release's events (see AggregateIDForRelease) share
// one lane.
const AggregateType = "agent_remediation"

var fixProposedNamespace = uuid.MustParse("d2a7f3c1-8e94-4b6a-9c2d-3f5b7a1e0d8c")

// RemediationEventID maps (releaseID, round, try) to a stable UUID so a
// redelivered trigger does not produce a distinct downstream event.
func RemediationEventID(releaseID string, round, try int) uuid.UUID {
	return uuid.NewSHA1(fixProposedNamespace, []byte(releaseID+"|"+itoa(round)+"|"+itoa(try)))
}

// AggregateIDForRelease maps a release id to a stable outbox AggregateID.
func AggregateIDForRelease(releaseID string) uuid.UUID {
	return uuid.NewSHA1(fixProposedNamespace, []byte("release:"+releaseID))
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
	Path         string
	ContentURI   string
	DiffURI      string
	TargetNodeID string
}

// FixProposed is the pointer-only trigger output: it carries S3 pointers
// to the proposed SQL + diff and the model's short rationale (no warehouse data).
type FixProposed struct {
	EventID   string
	Source    string
	ReleaseID string
	// RemediationRound is the release's remediation round this try belongs to.
	RemediationRound int
	NodeID           string
	// ResolvedNodeIDs is the failing nodes this try addresses, sorted.
	ResolvedNodeIDs []string
	ErrorSignature  string
	ProposedSQLURI  string
	DiffURI         string
	// Edits is the full multi-file description of this try's proposed
	// changes. Each edit names the node whose source it changes, which is what
	// tells a reader where the fix was made — one try repairs a whole
	// failing set, so no single node can stand for the cause of all of them.
	Edits          []ProposedEdit
	Rationale      string
	Confidence     string
	Model          string
	Try            int
	SourceResolved bool
	ProposedAt     string
}
