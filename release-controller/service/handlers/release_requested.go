package handlers

import (
	"context"
	"fmt"

	pkgevents "github.com/carolsimone/continuo/pkg/events"
	pkgoutbox "github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/carolsimone/continuo/release-controller/service/uow"
	"github.com/google/uuid"
)

// emitReleaseRequested maps the assembled manifest keys onto the wire payload
// and writes the release.requested:v1 outbox row. Shared by the compile-ok path
// (dbt) and AdvanceQueue's skip-compile activation branch (python) so the two
// emission sites cannot drift. imageTags is the run's assembled map; a nil map
// is sent as an empty object so the key is always present.
func emitReleaseRequested(ctx context.Context, u uow.UnitOfWork, releaseID string, keys []release.ManifestKey, imageTags map[string]string) error {
	manifestKeys := make([]pkgevents.ManifestKey, len(keys))
	for i, k := range keys {
		manifestKeys[i] = pkgevents.ManifestKey{Service: k.Service, S3URI: k.S3URI, Kind: string(k.Kind)}
	}
	payload, err := pkgevents.ReleaseRequestedPayload(pkgevents.ReleaseRequested{ReleaseID: releaseID, ManifestKeys: manifestKeys, ImageTags: imageTags})
	if err != nil {
		return err
	}
	if err := u.OutboxRepo().Create(ctx, &pkgoutbox.Entry{
		ID:            uuid.New(),
		AggregateType: "release-controller",
		AggregateID:   AggregateIDForRelease(releaseID),
		EventType:     "release_requested",
		Payload:       payload,
		StreamName:    streams.ReleaseRequestedV1,
		Status:        "pending",
	}); err != nil {
		return fmt.Errorf("outbox insert: %w", err)
	}
	return nil
}
