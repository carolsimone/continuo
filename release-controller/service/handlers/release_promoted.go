package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/carolsimone/continuo/pkg/events"
	pkgoutbox "github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/release-controller/domain/pipeline"
	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/carolsimone/continuo/release-controller/service/ports"
	"github.com/carolsimone/continuo/release-controller/service/uow"
	"github.com/google/uuid"
)

// enqueueReleasePromoted writes the release.promoted:v2 outbox row announcing
// p. The row's payload is p itself; the outbox relay wraps it in the event
// envelope when it publishes the row.
func enqueueReleasePromoted(ctx context.Context, u uow.UnitOfWork, p events.ReleasePromoted) error {
	if p.ChangedNodeIDs == nil {
		p.ChangedNodeIDs = []string{}
	}
	payload, err := json.Marshal(p)
	if err != nil {
		return fmt.Errorf("marshal release.promoted payload: %w", err)
	}
	if err := u.OutboxRepo().Create(ctx, &pkgoutbox.Entry{
		ID:            uuid.New(),
		AggregateType: "release-controller",
		AggregateID:   AggregateIDForRelease(p.ReleaseID),
		EventType:     ports.ReleasePromotedV2EventType,
		Payload:       payload,
		StreamName:    streams.ReleasePromotedV2,
		Status:        "pending",
	}); err != nil {
		return fmt.Errorf("outbox insert: %w", err)
	}
	return nil
}

// changedSincePromotion lists, sorted, the non-test nodes of r's candidate
// topology whose content_hash differs from, or is absent in, the production
// topology the promotion replaces (every node when there is no production yet).
// The list is never nil, so the event always carries a JSON array.
func changedSincePromotion(ctx context.Context, d *Deps, r *pipeline.Run, cp *release.CurrentProd) ([]string, error) {
	candidate, err := d.Topologies.Load(ctx, r.CandidateTopologyRef())
	if err != nil {
		return nil, fmt.Errorf("load candidate topology %s: %w", r.ID(), err)
	}
	prod, err := loadProdTopology(ctx, d, cp)
	if err != nil {
		return nil, err
	}
	changed := release.DerivedChangedNodeIDs(candidate.WithoutTests(), prod)
	if changed == nil {
		changed = []string{}
	}
	sort.Strings(changed)
	return changed, nil
}
