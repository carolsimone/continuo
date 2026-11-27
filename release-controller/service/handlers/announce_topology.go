package handlers

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/carolsimone/continuo/release-controller/service/uow"
)

// ErrNoLiveRelease reports that nothing has been promoted, so there is no
// live release whose topology could be re-announced or printed.
var ErrNoLiveRelease = errors.New("no live release to re-announce")

// ErrReleaseIDTaken reports an announcement under a release id that already
// names a release check or the live release: announcing under it would replace
// that release's topology artifact.
var ErrReleaseIDTaken = errors.New("release id already names a release check or the live release")

// releaseIDPattern is the release id rule of the public release API
// (RELEASE_ID_PATTERN in ui/src/server/routes/v1.ts); a test in pkg/streams
// keeps the two literals identical.
var releaseIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)

// ErrInvalidReleaseID reports a release id the public release API would
// refuse. The id becomes part of the artifact's object key, so it is checked
// before anything is written.
var ErrInvalidReleaseID = errors.New("release id must match " + releaseIDPattern.String())

// AnnounceResult is what an announcement published: the release id, the
// promotion seq it took and the topology artifact it points at.
type AnnounceResult struct {
	ReleaseID      string
	PromotionSeq   int64
	TopologyURI    string
	TopologySHA256 string
}

// AnnounceTopology announces topo to the rest of continuo as release
// releaseID: it writes the topology artifact, takes the next promotion seq and
// queues release.promoted:v1. The live release does not move, so the next real
// promotion is still diffed against the production release; it takes a
// higher seq and replaces the announced topology everywhere. The benchmark
// and e2e harnesses use it to load a topology without a release.
func AnnounceTopology(ctx context.Context, d *Deps, releaseID string, topo release.Topology) (AnnounceResult, error) {
	if !releaseIDPattern.MatchString(releaseID) {
		return AnnounceResult{}, fmt.Errorf("%w: %q", ErrInvalidReleaseID, releaseID)
	}
	// Catalog strips every dbt-test node before it applies a promotion, so a
	// topology that is all dbt-test nodes swaps in nothing and retires every live
	// node. Require at least one node that survives that filter.
	if len(topo.WithoutTests()) == 0 {
		return AnnounceResult{}, errors.New("announce topology: the topology has no node that survives the live-topology filter (every node is a dbt-test); announcing it would retire every live node")
	}

	u := d.NewUoW()
	if err := u.Begin(ctx); err != nil {
		return AnnounceResult{}, fmt.Errorf("begin tx: %w", err)
	}
	defer u.Rollback() //nolint:errcheck

	// Hold the release-queue lock across the live release's read, the seq
	// allocation and the announcement, so a concurrent promotion cannot commit
	// between them and be clobbered under a higher seq.
	if err := u.LockReleaseQueue(ctx); err != nil {
		return AnnounceResult{}, fmt.Errorf("lock release queue: %w", err)
	}

	check, err := u.ReleaseCheckRepo().Get(ctx, releaseID)
	if err != nil {
		return AnnounceResult{}, fmt.Errorf("get release %s: %w", releaseID, err)
	}
	live, err := u.LiveReleaseRepo().Get(ctx)
	if err != nil {
		return AnnounceResult{}, fmt.Errorf("get live release: %w", err)
	}
	if check != nil || live.ReleaseID() == releaseID {
		return AnnounceResult{}, fmt.Errorf("%w: %s", ErrReleaseIDTaken, releaseID)
	}

	ref, err := d.Topologies.Write(ctx, releaseID, topo)
	if err != nil {
		return AnnounceResult{}, fmt.Errorf("write topology artifact of %s: %w", releaseID, err)
	}
	seq, err := u.PromotionSequenceRepo().Next(ctx)
	if err != nil {
		return AnnounceResult{}, fmt.Errorf("take promotion seq: %w", err)
	}
	if err := enqueueReleasePromoted(ctx, u, events.ReleasePromoted{
		ReleaseID:      releaseID,
		PromotedAt:     d.Clock.Now().UTC(),
		PromotionSeq:   seq,
		TopologyURI:    ref.URI,
		TopologySHA256: ref.SHA256,
	}); err != nil {
		return AnnounceResult{}, err
	}
	if err := u.Commit(); err != nil {
		return AnnounceResult{}, fmt.Errorf("commit: %w", err)
	}
	d.Logger.Info("topology announced", "release_id", releaseID, "promotion_seq", seq, "topology_uri", ref.URI)
	return AnnounceResult{ReleaseID: releaseID, PromotionSeq: seq, TopologyURI: ref.URI, TopologySHA256: ref.SHA256}, nil
}

// ReannounceLiveRelease re-announces the live release's own topology artifact
// under the next promotion seq, so every consumer moves back to it from any
// topology announced since. The benchmark harness uses it to restore the live
// topology.
func ReannounceLiveRelease(ctx context.Context, d *Deps) (AnnounceResult, error) {
	u := d.NewUoW()
	if err := u.Begin(ctx); err != nil {
		return AnnounceResult{}, fmt.Errorf("begin tx: %w", err)
	}
	defer u.Rollback() //nolint:errcheck

	// Hold the release-queue lock across the live release's read, the seq
	// allocation and the re-announcement, so a concurrent promotion cannot
	// commit between them and be clobbered under a higher seq.
	if err := u.LockReleaseQueue(ctx); err != nil {
		return AnnounceResult{}, fmt.Errorf("lock release queue: %w", err)
	}

	live, err := u.LiveReleaseRepo().Get(ctx)
	if err != nil {
		return AnnounceResult{}, fmt.Errorf("get live release: %w", err)
	}
	if live.ReleaseID() == "" && live.Topology().IsZero() {
		return AnnounceResult{}, ErrNoLiveRelease
	}
	if live.Topology().IsZero() {
		return AnnounceResult{}, fmt.Errorf("live release %s references no topology artifact", live.ReleaseID())
	}
	seq, err := reannounce(ctx, u, live, d.Clock.Now())
	if err != nil {
		return AnnounceResult{}, err
	}
	if err := u.Commit(); err != nil {
		return AnnounceResult{}, fmt.Errorf("commit: %w", err)
	}
	ref := live.Topology()
	d.Logger.Info("live release re-announced", "release_id", live.ReleaseID(), "promotion_seq", seq)
	return AnnounceResult{ReleaseID: live.ReleaseID(), PromotionSeq: seq, TopologyURI: ref.URI, TopologySHA256: ref.SHA256}, nil
}

// LiveReleaseTopology returns the live release's topology, or
// ErrNoLiveRelease when nothing has been promoted.
func LiveReleaseTopology(ctx context.Context, d *Deps) (release.Topology, error) {
	live, err := d.NewUoW().LiveReleaseRepo().Get(ctx)
	if err != nil {
		return nil, fmt.Errorf("get live release: %w", err)
	}
	if live.ReleaseID() == "" && live.Topology().IsZero() {
		return nil, ErrNoLiveRelease
	}
	return loadLiveTopology(ctx, d, live)
}

// reannounce takes the next promotion seq, records it on live, persists live and
// queues release.promoted:v1 for live's release and artifact. A re-announcement
// changes nothing but the seq, so it names no changed node, no candidate
// schema and no code bundle.
func reannounce(ctx context.Context, u uow.UnitOfWork, live *release.LiveRelease, now time.Time) (int64, error) {
	seq, err := u.PromotionSequenceRepo().Next(ctx)
	if err != nil {
		return 0, fmt.Errorf("take promotion seq: %w", err)
	}
	if err := live.Reannounce(seq); err != nil {
		return 0, fmt.Errorf("re-announce live release: %w", err)
	}
	if err := u.LiveReleaseRepo().Upsert(ctx, live); err != nil {
		return 0, fmt.Errorf("upsert live release: %w", err)
	}
	ref := live.Topology()
	if err := enqueueReleasePromoted(ctx, u, events.ReleasePromoted{
		ReleaseID:      live.ReleaseID(),
		PromotedAt:     now.UTC(),
		PromotionSeq:   seq,
		TopologyURI:    ref.URI,
		TopologySHA256: ref.SHA256,
	}); err != nil {
		return 0, err
	}
	return seq, nil
}
