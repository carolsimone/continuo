package handlers

import "github.com/google/uuid"

// releaseAggregateNamespace is a fixed UUID-v4 used as the namespace for
// uuid.NewSHA1. Combined with the release_id (commit SHA) it yields a stable
// UUID v5 for outbox.AggregateID. The value must not change after deployment
// because existing outbox rows reference the derived UUIDs.
var releaseAggregateNamespace = uuid.MustParse("e7d1b2a4-5a3c-4f7e-bd1c-2f0a9b3c8d11")

// AggregateIDForRelease maps a release_id (commit SHA string) to a stable
// UUID for use as pkg/outbox.Entry.AggregateID. Identical input always
// produces identical output, so all outbox rows tied to a single release
// share an AggregateID.
func AggregateIDForRelease(releaseID string) uuid.UUID {
	return uuid.NewSHA1(releaseAggregateNamespace, []byte(releaseID))
}
