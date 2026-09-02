package ports

import (
	"context"
	"time"

	"github.com/carolsimone/continuo/agent-remediation/domain/proposal"
)

// VerificationRequest is a fix-verification run to submit: the edited
// service's delta, the rejected release the fix answers, and the attempt it
// belongs to. Kind is the service's manifest kind, VerificationKindDbt or
// VerificationKindPython — required, never defaulted. SourceOverlayURI is
// the tarball of proposed source a dbt run lays over the project; a python
// run is verified by its packaged contract and carries none.
type VerificationRequest struct {
	RunID             string
	Service           string
	ImageTag          string
	Kind              string
	VerifiesReleaseID string
	Attempt           int
	SourceOverlayURI  string
}

// Manifest kinds a verification request may carry.
const (
	VerificationKindDbt    = "dbt"
	VerificationKindPython = "python"
)

// VerificationStatus is one read of a verification run. Phase is derived
// from the run's status; ActivatedAt is when it left the pipeline's queue
// (zero while queued); NodeErrors is populated only when Phase is failed —
// one entry per failing validation node carrying the error text the next
// attempt is shown.
type VerificationStatus struct {
	Phase       proposal.Phase
	ActivatedAt time.Time
	NodeErrors  map[string]string
}

// VerificationPipeline is this service's client of the release pipeline for
// fix verification: submit a run, then read its status until it is terminal.
// Submit is idempotent on RunID (a 202 for an already-known id is success).
type VerificationPipeline interface {
	Submit(ctx context.Context, r VerificationRequest) error
	Status(ctx context.Context, runID string) (VerificationStatus, error)
}

// ReleaseReader reads facts about a candidate release: the image tag its
// changed service was posted with, which a verification of a fix to that
// service reuses verbatim (a verification never promotes, so the tag never
// reaches the path a promotion would drive), and the release's own failing
// validation nodes.
type ReleaseReader interface {
	ImageTag(ctx context.Context, releaseID, service string) (string, error)
	// FailingNodes returns the ORIGINAL candidate release's failing validation
	// nodes: node_id -> error text, one entry per node that failed validation
	// when that release was rejected. releaseID names the same failing
	// release ImageTag reads, not a verification run.
	FailingNodes(ctx context.Context, releaseID string) (map[string]string, error)
}
