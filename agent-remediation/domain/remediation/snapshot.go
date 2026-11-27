package remediation

import (
	"time"

	"github.com/google/uuid"

	"github.com/carolsimone/continuo/agent-remediation/domain/fix"
	"github.com/carolsimone/continuo/agent-remediation/domain/workflow"
)

// Remediation is the row of one remediation run: one per tenant, release and
// round.
type Remediation struct {
	ID        uuid.UUID
	TenantID  string
	ReleaseID string
	// Round is the release's round.
	Round  int
	Flow   string
	Status RemediationStatus
	// TriggerPayload is the raw payload of the heal trigger the run was
	// created from.
	TriggerPayload []byte
	// TriggerMessageID is the dedup id of the message the run answers to: the
	// heal trigger it was created from, or the redrive of that trigger's dead
	// letter that resumed it after a system failure.
	TriggerMessageID string
	// TriggerFields is that message's stream fields, without its payload.
	TriggerFields map[string]string
	CurrentTry    int
	// LeaseOwner and LeaseUntil name the worker holding the run and when its
	// lease ends. Both are empty while no worker holds it.
	LeaseOwner string
	LeaseUntil *time.Time
	Failure    string
	// Failures counts the drives in a row that failed before a step's row could
	// record the error: the remediation could not be loaded, or its next try
	// could not open. LastError is the error of the last of them. A try that
	// opens clears both, and so does a step that commits after a load that
	// found a try open.
	Failures  int
	LastError string
	CreatedAt time.Time
	UpdatedAt time.Time
}

// Try is the row of one try of a remediation run.
type Try struct {
	RemediationID uuid.UUID
	// Try is the try's number in its round, from 1, which keys its fix row
	// with the release and the round. It counts every try of the round, so a
	// try recorded with no remediation numbers the runner's next one too.
	Try    int
	Status TryStatus
	// FileRevisions maps each file name to the revision the try read it at.
	FileRevisions map[string]string
	DeadlineAt    time.Time
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

// Cluster is the row of one cluster of failing nodes inside a try.
type Cluster struct {
	RemediationID uuid.UUID
	Try           int
	Cluster       int
	Kind          workflow.ClusterKind
	TargetNodeID  string
	Members       []string
	TypeName      string
	Status        ClusterStatus
	// Draft is the number of the cluster's draft in its try, from 1: a
	// revision raises it.
	Draft int
	// Outcome is the cluster's result once it is handed over or fell back.
	Outcome   *fix.NodeOutcome
	UpdatedAt time.Time
}

// The journal rows a later try reads back from an earlier one, named as the
// runner writes them.
const (
	// StepPreflight is the step name of the runner's pre-flight pass of a
	// cluster: one row per draft, whose output is the draft the pass left and
	// whose findings are every finding of the pass.
	StepPreflight = "preflight"
	// KindRunner is the kind of the row of a step the runner performs itself.
	KindRunner = "runner"
	// KindCheck is the kind of a check's row: a pre-flight check, a
	// cross-cluster check or the gate.
	KindCheck = "check"
	// KindContext is the kind of a context step's row: what one context step
	// gathered for a cluster before its prompt.
	KindContext = "context"
	// FlowCluster is the cluster number of a flow-level row: a cross-cluster
	// check, the gate or an output.
	FlowCluster = 0
)

// StepRun is the row of one run of a step of a flow. Cluster is 0 for a
// flow-level step.
type StepRun struct {
	ID            int64
	RemediationID uuid.UUID
	Try           int
	Cluster       int
	Step          string
	// Kind is the kind of the step: context, prompt, check, approver, output
	// or runner.
	Kind   string
	Draft  int
	Status StepStatus
	// InputDigest identifies the inputs the step ran on.
	InputDigest string
	Output      []byte
	Findings    []Finding
	// WaitingOn lists the release check ids a waiting step waits for.
	WaitingOn     []string
	Model         string
	PromptVersion string
	TokensIn      int
	TokensOut     int
	CacheHit      bool
	FileRevision  string
	Failures      int
	LastError     string
	StartedAt     time.Time
	FinishedAt    *time.Time
}

// ShownToLaterTries reports whether a later try reads s back from an earlier
// one: a done pre-flight row of a cluster, or a done flow-level check row (a
// cross-cluster check or the gate).
func (s StepRun) ShownToLaterTries() bool {
	if s.Status != StepDone {
		return false
	}
	if s.Cluster == FlowCluster {
		return s.Kind == KindCheck
	}
	return s.Kind == KindRunner && s.Step == StepPreflight
}

// Snapshot is one remediation run as stored: the run, one of its tries (its
// current try, unless the reader says otherwise), and the clusters and steps of
// that try.
type Snapshot struct {
	Remediation Remediation
	Try         *Try
	Clusters    []Cluster
	Steps       []StepRun
}
