package ports

import (
	"context"
	"time"

	"github.com/carolsimone/continuo/agent-remediation/domain/proposal"
)

// ReleaseCheckRequest is a fix's release check to submit: the edited
// project's delta, the rejected release the fix answers, and the attempt it
// belongs to. Kind is the project's manifest kind, ManifestKindDbt or
// ManifestKindPython — required, never defaulted. SourceOverlayURI is
// the tarball of proposed source a dbt check lays over the project; a python check
// is verified by its packaged odcs-bundle.json and carries none.
type ReleaseCheckRequest struct {
	CheckID           string
	Project           string
	ImageTag          string
	Kind              string
	VerifiesReleaseID string
	Attempt           int
	SourceOverlayURI  string
}

// Manifest kinds a release check request may carry.
const (
	ManifestKindDbt    = "dbt"
	ManifestKindPython = "python"
)

// ODCSBundleName is the object name of a Python project's bundle of
// ODCS contracts: its CI uploads one for a release at
// <project>/<release id>/odcs-bundle.json, and a fix's release check reads
// the packed one at <project>/<check id>/odcs-bundle.json.
const ODCSBundleName = "odcs-bundle.json"

// ReleaseCheckStatus is one read of a release check. Phase is derived
// from the check's status; ActivatedAt is when it left the release queue
// (zero while queued). NodeErrors and CheckError are set only when Phase is
// failed: NodeErrors holds one entry per node the failure names, carrying the
// error text the next attempt is shown, and CheckError is the check's own reason
// and detail ("reason — detail"), the text of a failure that names no node.
type ReleaseCheckStatus struct {
	Phase       proposal.Phase
	ActivatedAt time.Time
	NodeErrors  map[string]string
	CheckError  string
}

// ReleaseChecks is this component's client of release-controller for the
// release checks of fixes: submit a check, then read its status until it is
// terminal.
// Submit is idempotent on CheckID (a 202 for an already-known id is success).
type ReleaseChecks interface {
	Submit(ctx context.Context, r ReleaseCheckRequest) error
	Status(ctx context.Context, checkID string) (ReleaseCheckStatus, error)
}

// ContractBundleReader reads where a candidate release's contract bundle is.
type ContractBundleReader interface {
	// ContractBundle returns the URI and SHA-256 of the contract bundle the
	// release's parse wrote. An empty uri means the release has none: it was
	// rejected before a parse ended ok.
	ContractBundle(ctx context.Context, releaseID string) (uri, sha256 string, err error)
}

// ReleaseReader reads facts about a candidate release: where its contract
// bundle is, the image tag its changed project was posted with, which the
// release check of a fix to that project reuses verbatim (a fix's release check
// never promotes, so the tag never reaches the path a promotion would drive), and
// the release's own failing validation nodes.
type ReleaseReader interface {
	ContractBundleReader
	ImageTag(ctx context.Context, releaseID, project string) (string, error)
	// FailingNodes returns the ORIGINAL candidate release's failing validation
	// nodes: node_id -> error text, one entry per node that failed validation
	// when that release was rejected. releaseID names the same failing
	// release ImageTag reads, not a release check.
	FailingNodes(ctx context.Context, releaseID string) (map[string]string, error)
}
