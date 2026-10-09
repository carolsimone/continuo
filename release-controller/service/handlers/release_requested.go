package handlers

import (
	"context"
	"encoding/json"
	"fmt"

	pkgoutbox "github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/carolsimone/continuo/release-controller/service/uow"
	"github.com/google/uuid"
)

// manifestKeyDTO is the wire shape for one service's artifact entry in the
// release.requested:v1 payload. Kind is emitted explicitly for every entry
// ("dbt" | "python"): topology-controller's absent-means-dbt defaulting is
// decoder tolerance for old payloads, not an emission convention.
type manifestKeyDTO struct {
	Service string `json:"service"`
	S3URI   string `json:"s3_uri"`
	Kind    string `json:"kind"`
}

// releaseRequestedPayload is the exact wire shape of release.requested:v1 as
// consumed by topology-controller. ImageTags is the run's assembled map
// (service -> tag): topology-controller joins it onto every node of the
// candidate topology artifact it writes, so the artifact carries the image
// each node runs with.
type releaseRequestedPayload struct {
	ReleaseID    string            `json:"release_id"`
	ManifestKeys []manifestKeyDTO  `json:"manifest_keys"`
	ImageTags    map[string]string `json:"image_tags"`
}

// emitReleaseRequested maps the assembled manifest keys onto the wire DTO and
// writes the release.requested:v1 outbox row. Shared by the compile-ok path
// (dbt) and AdvanceQueue's skip-compile activation branch (python) so the two
// emission sites cannot drift. imageTags is the run's assembled map; a nil map
// is sent as an empty object so the key is always present.
func emitReleaseRequested(ctx context.Context, u uow.UnitOfWork, releaseID string, keys []release.ManifestKey, imageTags map[string]string) error {
	dtos := make([]manifestKeyDTO, len(keys))
	for i, k := range keys {
		dtos[i] = manifestKeyDTO{Service: k.Service, S3URI: k.S3URI, Kind: string(k.Kind)}
	}
	if imageTags == nil {
		imageTags = map[string]string{}
	}
	payload, err := json.Marshal(releaseRequestedPayload{ReleaseID: releaseID, ManifestKeys: dtos, ImageTags: imageTags})
	if err != nil {
		return fmt.Errorf("marshal release.requested payload: %w", err)
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
