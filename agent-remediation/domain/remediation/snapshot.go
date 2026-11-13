package remediation

import (
	"time"

	"github.com/google/uuid"

	"github.com/carolsimone/continuo/agent-remediation/domain/proposal"
	"github.com/carolsimone/continuo/agent-remediation/domain/workflow"
)

// Remediation is the row of one remediation run: one per tenant, release and
// round.
type Remediation struct {
	ID        uuid.UUID
	TenantID  string
	ReleaseID string
	// Round is the release's remediation round.
	Round  int
	Flow   string
	Status RemediationStatus
	// TriggerPayload is the raw payload of the heal trigger the run was
	// created from.
	TriggerPayload   []byte
	TriggerMessageID string
	// TriggerFields is the trigger message's stream fields.
	TriggerFields map[string]string
	CurrentTry    int
	// LeaseOwner and LeaseUntil name the worker holding the run and when its
	// lease ends. Both are empty while no worker holds it.
	LeaseOwner string
	LeaseUntil *time.Time
	Failure    string
	CreatedAt  time.Time
	UpdatedAt  time.Time
}

// Try is the row of one try of a remediation run.
type Try struct {
	RemediationID uuid.UUID
	Try           int
	// Attempt is the proposal attempt number of the try.
	Attempt int
	Status  TryStatus
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
	// Round is the cluster's revision round.
	Round int
	// Outcome is the cluster's result once it is handed over or fell back.
	Outcome   *proposal.NodeOutcome
	UpdatedAt time.Time
}

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
	Round  int
	Status StepStatus
	// InputDigest identifies the inputs the step ran on.
	InputDigest string
	Output      []byte
	Findings    []Finding
	// WaitingOn lists the verification run ids a waiting step waits for.
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

// Snapshot is one remediation run as stored: the run, its current try, and the
// clusters and steps of that try.
type Snapshot struct {
	Remediation Remediation
	Try         *Try
	Clusters    []Cluster
	Steps       []StepRun
}
