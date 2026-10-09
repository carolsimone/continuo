package handlers

import (
	"context"
	"errors"
	"fmt"
	"regexp"

	"github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/release-controller/domain/release"
)

// ErrNoCurrentProd reports that nothing has been promoted, so there is no
// current_prod topology to re-announce or print.
var ErrNoCurrentProd = errors.New("no current_prod to re-announce")

// ErrReleaseIDTaken reports an announcement under a release id that already
// names a pipeline run or current_prod: announcing under it would replace that
// release's topology artifact.
var ErrReleaseIDTaken = errors.New("release id already names a pipeline run or current_prod")

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
// queues release.promoted:v2. current_prod does not move, so the next real
// promotion is still diffed against the production release; it takes a
// higher seq and replaces the announced topology everywhere. The benchmark
// and e2e harnesses use it to load a topology without a release.
func AnnounceTopology(ctx context.Context, d *Deps, releaseID string, topo release.Topology) (AnnounceResult, error) {
	if !releaseIDPattern.MatchString(releaseID) {
		return AnnounceResult{}, fmt.Errorf("%w: %q", ErrInvalidReleaseID, releaseID)
	}
	// Orchestrator strips every dbt-test node before it applies a promotion, so a
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

	// Hold the release-queue lock across the current_prod read, the seq
	// allocation and the announcement, so a concurrent promotion cannot commit
	// between them and be clobbered under a higher seq.
	if err := u.LockReleaseQueue(ctx); err != nil {
		return AnnounceResult{}, fmt.Errorf("lock release queue: %w", err)
	}

	run, err := u.RunRepo().Get(ctx, releaseID)
	if err != nil {
		return AnnounceResult{}, fmt.Errorf("get run %s: %w", releaseID, err)
	}
	cp, err := u.CurrentProdRepo().Get(ctx)
	if err != nil {
		return AnnounceResult{}, fmt.Errorf("get current prod: %w", err)
	}
	if run != nil || cp.ReleaseID() == releaseID {
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

// ReannounceCurrentProd re-announces current_prod's own topology artifact
// under the next promotion seq, so every consumer moves back to it from any
// topology announced since. The benchmark harness uses it to restore the live
// topology.
func ReannounceCurrentProd(ctx context.Context, d *Deps) (AnnounceResult, error) {
	u := d.NewUoW()
	if err := u.Begin(ctx); err != nil {
		return AnnounceResult{}, fmt.Errorf("begin tx: %w", err)
	}
	defer u.Rollback() //nolint:errcheck

	// Hold the release-queue lock across the current_prod read, the seq
	// allocation and the re-announcement, so a concurrent promotion cannot
	// commit between them and be clobbered under a higher seq.
	if err := u.LockReleaseQueue(ctx); err != nil {
		return AnnounceResult{}, fmt.Errorf("lock release queue: %w", err)
	}

	cp, err := u.CurrentProdRepo().Get(ctx)
	if err != nil {
		return AnnounceResult{}, fmt.Errorf("get current prod: %w", err)
	}
	if cp.ReleaseID() == "" && cp.Topology().IsZero() {
		return AnnounceResult{}, ErrNoCurrentProd
	}
	if cp.Topology().IsZero() {
		return AnnounceResult{}, fmt.Errorf("current_prod %s references no topology artifact; start release-controller once so its startup step writes it", cp.ReleaseID())
	}
	seq, err := reannounce(ctx, u, cp, d.Clock.Now())
	if err != nil {
		return AnnounceResult{}, err
	}
	if err := u.Commit(); err != nil {
		return AnnounceResult{}, fmt.Errorf("commit: %w", err)
	}
	ref := cp.Topology()
	d.Logger.Info("current_prod re-announced", "release_id", cp.ReleaseID(), "promotion_seq", seq)
	return AnnounceResult{ReleaseID: cp.ReleaseID(), PromotionSeq: seq, TopologyURI: ref.URI, TopologySHA256: ref.SHA256}, nil
}

// CurrentProdTopology returns the topology current_prod points at, or
// ErrNoCurrentProd when nothing has been promoted.
func CurrentProdTopology(ctx context.Context, d *Deps) (release.Topology, error) {
	cp, err := d.NewUoW().CurrentProdRepo().Get(ctx)
	if err != nil {
		return nil, fmt.Errorf("get current prod: %w", err)
	}
	if cp.ReleaseID() == "" && cp.Topology().IsZero() {
		return nil, ErrNoCurrentProd
	}
	return loadProdTopology(ctx, d, cp)
}
