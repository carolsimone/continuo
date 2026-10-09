package handlers

import (
	"context"
	"fmt"
	"time"

	"github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/carolsimone/continuo/release-controller/service/uow"
)

// BackfillCurrentProdArtifact repairs a current_prod that names a release but
// references no topology artifact: it writes the release's legacy snapshot as
// the release's topology artifact, points current_prod at it, and
// re-announces the release on release.promoted:v2 under the next promotion
// seq, so every consumer records the seq the live topology carries.
//
// It runs under the release-queue lock, so replicas starting together
// serialise on it: the first backfills and re-announces, and the others then
// find a current_prod that references an artifact and do nothing. It does
// nothing when current_prod is empty or already references an artifact, so it
// runs at every start. The artifact bytes are deterministic, so a crash
// between writing the artifact and recording its reference is repaired by the
// retry writing the same object under the same key.
func BackfillCurrentProdArtifact(ctx context.Context, d *Deps) error {
	u := d.NewUoW()
	if err := u.Begin(ctx); err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer u.Rollback() //nolint:errcheck

	if err := u.LockReleaseQueue(ctx); err != nil {
		return fmt.Errorf("lock release queue: %w", err)
	}

	legacy, err := u.LegacyTopologyRepo().GetLegacyCurrentProd(ctx)
	if err != nil {
		return fmt.Errorf("read legacy current_prod: %w", err)
	}
	if legacy == nil {
		return u.Commit()
	}

	ref, err := d.Topologies.Write(ctx, legacy.ReleaseID, legacy.Topology)
	if err != nil {
		return fmt.Errorf("write topology artifact of current_prod %s: %w", legacy.ReleaseID, err)
	}
	cp, err := u.CurrentProdRepo().Get(ctx)
	if err != nil {
		return fmt.Errorf("get current prod: %w", err)
	}
	cp.RecordArtifact(ref)
	seq, err := reannounce(ctx, u, cp, d.Clock.Now())
	if err != nil {
		return err
	}
	if err := u.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	d.Logger.Info("current_prod topology artifact written and re-announced",
		"release_id", cp.ReleaseID(), "promotion_seq", seq, "topology_uri", ref.URI)
	return nil
}

// reannounce takes the next promotion seq, records it on cp, persists cp and
// queues release.promoted:v2 for cp's release and artifact. A re-announcement
// changes nothing but the seq, so it names no changed node, no candidate
// schema and no code bundle.
func reannounce(ctx context.Context, u uow.UnitOfWork, cp *release.CurrentProd, now time.Time) (int64, error) {
	seq, err := u.PromotionSequenceRepo().Next(ctx)
	if err != nil {
		return 0, fmt.Errorf("take promotion seq: %w", err)
	}
	if err := cp.Reannounce(seq); err != nil {
		return 0, fmt.Errorf("re-announce current prod: %w", err)
	}
	if err := u.CurrentProdRepo().Upsert(ctx, cp); err != nil {
		return 0, fmt.Errorf("upsert current prod: %w", err)
	}
	ref := cp.Topology()
	if err := enqueueReleasePromoted(ctx, u, events.ReleasePromoted{
		ReleaseID:      cp.ReleaseID(),
		PromotedAt:     now.UTC(),
		PromotionSeq:   seq,
		TopologyURI:    ref.URI,
		TopologySHA256: ref.SHA256,
	}); err != nil {
		return 0, err
	}
	return seq, nil
}
