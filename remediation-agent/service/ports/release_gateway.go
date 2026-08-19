package ports

import "context"

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
}

// ReleaseGateway is remediation-agent's client of release-controller's public
// HTTP API for shadow verification: the python fixer packages a corrected
// contract, submits it as a shadow release, then polls for its verdict.
//
// Submit and Verdict both take the SHADOW release's own id. ImageTag takes a
// different id on purpose: it is called with the id of the ORIGINAL, failing
// release being fixed, not the shadow. The remediation.requested trigger
// carries no image tag, so the caller reads the failing release's tag once
// via ImageTag(ctx, originalReleaseID, service) and reuses it verbatim as the
// ImageTag field of the ShadowSubmission it then Submits under the shadow's
// own, newly minted release id. Reusing the original tag is safe because a
// shadow release never promotes, so it never reaches the code path
// (current_prod assembly) an image tag would otherwise drive.
type ReleaseGateway interface {
	// Submit posts a shadow verification release. A 202 Accepted response —
	// including release-controller's idempotent-duplicate case, where a row
	// for this release id already exists — is success.
	Submit(ctx context.Context, s ShadowSubmission) error
	// Verdict reads the shadow release identified by releaseID and reports
	// its current status, decoded into a ShadowVerdict.
	Verdict(ctx context.Context, releaseID string) (ShadowVerdict, error)
	// ImageTag returns image_tags[service] from GET /releases/{releaseID} —
	// releaseID here is the ORIGINAL failing release, not a shadow. An
	// unknown service (absent from that release's image_tags) is an error.
	ImageTag(ctx context.Context, releaseID, service string) (string, error)
}
