package ports

import (
	"context"
	"time"
)

// ShadowSubmission is the release-controller POST /releases body for a shadow
// verification release: a real release that runs the full parse →
// candidate-schema → validation pipeline but stops at the terminal
// "validated" status instead of promoting. Kind is always "python" (the
// python fixer's corrected artifact is always a contract yaml), Bootstrap is
// always false, and Shadow is always true — none of the three is exposed as a
// field because a shadow submission never varies them.
type ShadowSubmission struct {
	ReleaseID string
	Service   string
	ImageTag  string
	Repo      string
	CommitSHA string
}

// ShadowVerdict is the outcome of a shadow release, read from GET
// /releases/{id}. Terminal is false while the release is still in a
// non-terminal status (received/compiling/parsing/seed_building/validating);
// once Terminal is true, Validated distinguishes a passing shadow
// ("validated") from a rejected one ("rejected"). NodeErrors is populated
// only on a rejected, terminal verdict: one entry per failing validation
// node, keyed by unique_id, carrying the error text the next fix attempt uses
// as evidence.
type ShadowVerdict struct {
	Terminal   bool
	Validated  bool
	NodeErrors map[string]string
	// ActivatedAt is when the release left the queue — the moment of its first
	// transition past "received" — and zero while it is still waiting its turn.
	//
	// A shadow release joins the same global FIFO queue every other release
	// does, and only one release runs at a time, so an arbitrary amount of the
	// wall-clock time since submission can be time the release has not been
	// running at all. A caller bounding how long verification may take measures
	// from this, so a backlog cannot consume the budget of a release that never
	// started.
	ActivatedAt time.Time
}

// ReleaseGateway is agent-remediation's client of release-controller's public
// HTTP API for shadow verification: the python fixer packages a corrected
// contract, submits it as a shadow release, then polls for its verdict.
//
// Two distinct release-id spaces travel through this port, and which one a
// method takes is part of its contract. Submit always names the SHADOW release
// being posted. ImageTag always names the ORIGINAL, failing release being
// fixed: the remediation.requested trigger carries no image tag, so the caller
// reads the failing release's tag once via ImageTag(ctx, originalReleaseID,
// service) and reuses it verbatim as the ImageTag field of the ShadowSubmission
// it then Submits under the shadow's own, newly minted id. Reusing the original
// tag is safe because a shadow release never promotes, so it never reaches the
// code path (current_prod assembly) an image tag would otherwise drive.
//
// Verdict takes either, because "which nodes did this release reject, and why"
// is the same question for both: the reconciler asks it of a SHADOW release to
// resolve a waiting attempt, and the fixer asks it of the ORIGINAL release to
// find out whether another node it would package alongside the one it is fixing
// also failed.
type ReleaseGateway interface {
	// Submit posts a shadow verification release. A 202 Accepted response —
	// including release-controller's idempotent-duplicate case, where a row
	// for this release id already exists — is success.
	Submit(ctx context.Context, s ShadowSubmission) error
	// Verdict reads the release identified by releaseID — a shadow or the
	// original failing one — and reports its current status, decoded into a
	// ShadowVerdict.
	Verdict(ctx context.Context, releaseID string) (ShadowVerdict, error)
	// ImageTag returns image_tags[service] from GET /releases/{releaseID} —
	// releaseID here is the ORIGINAL failing release, not a shadow. An
	// unknown service (absent from that release's image_tags) is an error.
	ImageTag(ctx context.Context, releaseID, service string) (string, error)
}
