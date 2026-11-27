package fixes_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/domain/event"
	"github.com/carolsimone/continuo/agent-remediation/domain/fix"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/domain/repository"
	"github.com/carolsimone/continuo/agent-remediation/serialization"
	"github.com/carolsimone/continuo/agent-remediation/service/fixes"
	"github.com/carolsimone/continuo/agent-remediation/service/uow"
	"github.com/carolsimone/continuo/pkg/messageprocessing"
	"github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/pkg/streams"
)

// fixedClock returns a fixed time for deterministic tests.
type fixedClock struct{}

func (fixedClock) Now() time.Time {
	t, _ := time.Parse(time.RFC3339, "2026-06-24T00:00:00Z")
	return t
}

// fakeRepo implements repository.FixRepository and doubles as a UoW factory.
type fakeRepo struct {
	// state fed to Get/List
	view fix.View
	// captured outputs
	lastBranch    string
	lastClaimedAt time.Time
	lastOutbox    *outbox.Entry
	committed     bool
	// BeginPR capture
	lastBeginProject string
	// RecordPR capture
	lastRecordPR struct {
		id       string
		project  string
		prURL    string
		prNumber int
		openedBy string
		openedAt time.Time
	}
	// recordPRCASHit controls RecordPR's returned hit value; defaults to true
	// so existing tests that do not care about the CAS outcome keep passing.
	recordPRCASHit bool
	// RecordPROutcome capture
	lastOutcome        fix.PROutcome
	lastOutcomeProject string
	lastClosedAt       time.Time
	outcomeCASHit      bool
	openPRs            []fix.OpenPR
	// FailStuckOpeningPR capture
	lastFailStuckID        string
	lastFailStuckProject   string
	lastFailStuckClaimedAt time.Time
	failStuckHit           bool
}

func (r *fakeRepo) CountAttempts(_ context.Context, _ string, _ int) (int, error) {
	return 0, nil
}
func (r *fakeRepo) InsertGenerating(_ context.Context, _ fix.Fix) error { return nil }
func (r *fakeRepo) FailGenerating(_ context.Context, _, _ string) (int, error) {
	return 0, nil
}
func (r *fakeRepo) FailLegacyInFlight(_ context.Context, _ string) (int, error) {
	return 0, nil
}
func (r *fakeRepo) Upsert(_ context.Context, _ fix.Fix) error { return nil }
func (r *fakeRepo) Get(_ context.Context, _ string) (fix.View, error) {
	return r.view, nil
}
func (r *fakeRepo) List(_ context.Context, _ repository.FixFilter) ([]fix.View, error) {
	return []fix.View{r.view}, nil
}
func (r *fakeRepo) BeginPR(_ context.Context, _, project string, branch string, claimedAt time.Time) (fix.PRClaim, error) {
	r.lastBeginProject = project
	r.lastBranch = branch
	r.lastClaimedAt = claimedAt
	return fix.PRClaim{
		ID:        r.view.ID,
		ReleaseID: r.view.ReleaseID,
		NodeID:    r.view.NodeID,
		Attempt:   r.view.Attempt,
		Project:   project,
	}, nil
}
func (r *fakeRepo) RecordPR(_ context.Context, id, project, prURL string, prNumber int, openedBy string, openedAt time.Time) (bool, error) {
	r.lastRecordPR.id = id
	r.lastRecordPR.project = project
	r.lastRecordPR.prURL = prURL
	r.lastRecordPR.prNumber = prNumber
	r.lastRecordPR.openedBy = openedBy
	r.lastRecordPR.openedAt = openedAt
	return r.recordPRCASHit, nil
}
func (r *fakeRepo) FailStuckOpeningPR(_ context.Context, id, project string, observedClaimedAt time.Time) (bool, error) {
	r.lastFailStuckID = id
	r.lastFailStuckProject = project
	r.lastFailStuckClaimedAt = observedClaimedAt
	return r.failStuckHit, nil
}
func (r *fakeRepo) ListOpenPullRequests(_ context.Context, _ int) ([]fix.OpenPR, error) {
	return r.openPRs, nil
}
func (r *fakeRepo) ListStuckOpening(_ context.Context, _ int, _ *repository.OpeningCursor) ([]fix.OpeningPR, *repository.OpeningCursor, error) {
	return nil, nil, nil
}
func (r *fakeRepo) RecordPROutcome(_ context.Context, _, project string, outcome fix.PROutcome, closedAt time.Time) (bool, error) {
	r.lastOutcome = outcome
	r.lastOutcomeProject = project
	r.lastClosedAt = closedAt
	return r.outcomeCASHit, nil
}

func (r *fakeRepo) MarkVerifyFailed(_ context.Context, _, _ string, _ []fix.ReleaseCheck) (bool, error) {
	return false, nil
}
func (r *fakeRepo) MarkFixVerified(_ context.Context, _ remediation.Verified) (bool, error) {
	return false, nil
}

func (r *fakeRepo) RecordApprovers(_ context.Context, _ string, _ int, _ []fix.Approver) error {
	return nil
}
func (r *fakeRepo) ReopenTryAttempt(_ context.Context, _ uuid.UUID, _ int, _ fix.Status, _ map[string]fix.NodeOutcome) (bool, error) {
	return false, nil
}

func (r *fakeRepo) UpdateReleaseCheckPhase(_ context.Context, _, _ string, _ fix.Phase, _ *time.Time) error {
	return nil
}

// fakeUoW is a unit of work backed by the fakeRepo.
type fakeUoW struct {
	repo *fakeRepo
}

func (u *fakeUoW) Begin(_ context.Context) error                       { return nil }
func (u *fakeUoW) Commit() error                                       { u.repo.committed = true; return nil }
func (u *fakeUoW) Rollback() error                                     { return nil }
func (u *fakeUoW) FixRepo() repository.FixRepository                   { return u.repo }
func (u *fakeUoW) RemediationRepo() repository.RemediationRepository   { return nil }
func (u *fakeUoW) OutboxRepo() outbox.Repository                       { return &fakeOutboxRepo{repo: u.repo} }
func (u *fakeUoW) MessageProcessingRepo() messageprocessing.Repository { return nil }

// fakeOutboxRepo captures the outbox entry passed to Create.
type fakeOutboxRepo struct {
	repo *fakeRepo
}

func (o *fakeOutboxRepo) Create(_ context.Context, e *outbox.Entry) error {
	o.repo.lastOutbox = e
	return nil
}
func (o *fakeOutboxRepo) GetPendingBatch(_ context.Context, _ int) ([]*outbox.Entry, error) {
	return nil, nil
}
func (o *fakeOutboxRepo) MarkProcessed(_ context.Context, _ uuid.UUID) error        { return nil }
func (o *fakeOutboxRepo) MarkProcessedBatch(_ context.Context, _ []uuid.UUID) error { return nil }
func (o *fakeOutboxRepo) MarkFailed(_ context.Context, _ uuid.UUID, _ string) error { return nil }
func (o *fakeOutboxRepo) IncrementRetry(_ context.Context, _ uuid.UUID) error       { return nil }

func (r *fakeRepo) uowFactory() uow.UnitOfWork {
	return &fakeUoW{repo: r}
}

// TestBuildBranch_DistinctPerAttemptWithinARelease verifies that, with the
// node segment gone, attempt is still enough to keep two attempts of the same
// release on distinct branches, and that a legacy project "" carries no
// project suffix.
func TestBuildBranch_DistinctPerAttemptWithinARelease(t *testing.T) {
	b1 := fixes.BuildBranch("r", 1, "")
	b2 := fixes.BuildBranch("r", 2, "")
	require.Equal(t, "remediation/r/attempt1", b1)
	require.Equal(t, "remediation/r/attempt2", b2)
	require.NotEqual(t, b1, b2, "two attempts of the same release must not collide on branch")
}

// TestBuildBranch_PerProjectSuffix verifies a non-empty project appends a
// "/<project>" segment, and the empty legacy project does not, so each owning
// project's PR lands on its own branch.
func TestBuildBranch_PerProjectSuffix(t *testing.T) {
	require.Equal(t, "remediation/rel-1/attempt2/core", fixes.BuildBranch("rel-1", 2, "core"))
	require.Equal(t, "remediation/rel-1/attempt2", fixes.BuildBranch("rel-1", 2, ""))
	require.NotEqual(t,
		fixes.BuildBranch("rel-1", 2, "core"),
		fixes.BuildBranch("rel-1", 2, "finance"),
		"two projects of the same attempt must not collide on branch")
}

// TestService_Begin_BuildsDeterministicBranch verifies that Begin on a legacy
// (unsplit) fix derives the branch name remediation/<release_id>/attempt<n>,
// with no project segment, and stamps the claim with the service clock.
func TestService_Begin_BuildsDeterministicBranch(t *testing.T) {
	repo := &fakeRepo{view: fix.View{
		ReleaseID:      "r-1",
		NodeID:         "model.p.orders_d",
		Attempt:        1,
		Repo:           "org/repo",
		SourceResolved: true,
	}}
	svc := fixes.New(fixes.Deps{
		Repo:   repo,
		NewUoW: repo.uowFactory,
		Clock:  fixedClock{},
	})
	_, err := svc.Begin(context.Background(), "p1", "")
	require.NoError(t, err)
	require.Equal(t, "remediation/r-1/attempt1", repo.lastBranch)
	require.Equal(t, "", repo.lastBeginProject, "a legacy fix claims the whole-fix \"\" group")
	require.Equal(t, fixedClock{}.Now(), repo.lastClaimedAt, "Begin must stamp the claim with the service's clock")
}

// TestService_Begin_ThreadsProject verifies Begin passes the requested owning
// project through to the repository and builds that project's branch, when the
// fix's edits attribute an edit to that project.
func TestService_Begin_ThreadsProject(t *testing.T) {
	repo := &fakeRepo{view: fix.View{
		ID:             "p1",
		ReleaseID:      "r-1",
		Attempt:        1,
		Repo:           "org/repo",
		SourceResolved: true,
		Edits: []fix.FileEdit{
			{Path: "services/core/models/a.sql", MemberNodeIDs: []string{"model.core.a"}},
		},
	}}
	svc := fixes.New(fixes.Deps{
		Repo:             repo,
		NewUoW:           repo.uowFactory,
		Clock:            fixedClock{},
		ProjectRepoPaths: map[string]string{"core": "services/core"},
	})

	claim, err := svc.Begin(context.Background(), "p1", "core")
	require.NoError(t, err)
	require.Equal(t, "core", repo.lastBeginProject, "Begin must thread the requested project to BeginPR")
	require.Equal(t, "remediation/r-1/attempt1/core", repo.lastBranch)
	require.Equal(t, "remediation/r-1/attempt1/core", claim.Branch, "the returned claim carries the per-project branch")
}

// TestService_Begin_UnknownProjectRejected verifies Begin rejects a project the
// fix has no edits for: a split fix (its edits attribute members) can
// only be claimed on one of its real owning-project keys, never on "" or a name
// with no edits.
func TestService_Begin_UnknownProjectRejected(t *testing.T) {
	repo := &fakeRepo{view: fix.View{
		ID:             "p1",
		ReleaseID:      "r-1",
		Attempt:        1,
		SourceResolved: true,
		Edits: []fix.FileEdit{
			{Path: "services/core/models/a.sql", MemberNodeIDs: []string{"model.core.a"}},
		},
	}}
	svc := fixes.New(fixes.Deps{
		Repo:             repo,
		NewUoW:           repo.uowFactory,
		Clock:            fixedClock{},
		ProjectRepoPaths: map[string]string{"core": "services/core"},
	})

	_, err := svc.Begin(context.Background(), "p1", "finance")
	require.ErrorIs(t, err, fixes.ErrUnknownProject)
	require.Empty(t, repo.lastBeginProject, "a rejected project must never reach the repository")

	// A split fix has no legacy "" group, so "" is also rejected.
	_, err = svc.Begin(context.Background(), "p1", "")
	require.ErrorIs(t, err, fixes.ErrUnknownProject)
}

// TestService_Begin_NoRepositoryRefused verifies Begin refuses to claim a
// fix whose Repo is not owner/name: a fix with no repository can
// never open a pull request, so the claim is refused before any row is
// touched, and a row a sweep has already failed cannot be walked back into
// 'opening' by claiming it here.
func TestService_Begin_NoRepositoryRefused(t *testing.T) {
	repo := &fakeRepo{view: fix.View{
		ID:             "p1",
		ReleaseID:      "r-1",
		Attempt:        1,
		Repo:           "",
		SourceResolved: true,
		Status:         fix.StatusProposed,
	}}
	svc := fixes.New(fixes.Deps{
		Repo:   repo,
		NewUoW: repo.uowFactory,
		Clock:  fixedClock{},
	})

	_, err := svc.Begin(context.Background(), "p1", "")

	require.ErrorIs(t, err, fixes.ErrNoRepository)
	require.Empty(t, repo.lastBranch, "no claim is attempted")
}

// TestService_PRProjects_LegacyFixSingleGroup verifies ruling 1: a
// fix whose edits carry NO members is never split — PRProjects returns the
// single legacy [""] group — while a fix whose edits attribute members
// returns the sorted owning-project keys.
func TestService_PRProjects_LegacyFixSingleGroup(t *testing.T) {
	svc := fixes.New(fixes.Deps{
		Repo:             &fakeRepo{},
		NewUoW:           (&fakeRepo{}).uowFactory,
		Clock:            fixedClock{},
		ProjectRepoPaths: map[string]string{"core": "services/core", "finance": "services/finance"},
	})

	legacy := fix.View{Edits: []fix.FileEdit{
		{Path: "services/core/models/a.sql"}, // no MemberNodeIDs
	}}
	require.Equal(t, []string{""}, svc.PRProjects(legacy),
		"a fix with no per-edit members must never be split")

	split := fix.View{Edits: []fix.FileEdit{
		{Path: "services/core/models/a.sql", MemberNodeIDs: []string{"model.core.a"}},
		{Path: "services/finance/models/b.sql", MemberNodeIDs: []string{"model.finance.b"}},
	}}
	require.Equal(t, []string{"core", "finance"}, svc.PRProjects(split),
		"a fix whose edits attribute members returns the sorted owning-project keys")
}

// TestService_PRProjects_CollapsesWhenAnEditIsUnmapped verifies the fix for
// the legacy "" key being overloaded: it means both "this fix was never
// split" and "this edit's path matched no configured project". When
// ProjectRepoPaths is missing an entry for one of a member-attributed
// fix's edits, GroupEditsByProject buckets that edit under "" while the
// other, mapped edit gets its own named key. Splitting on both groups as
// written would open a "" pull request (which the repository's toClaim
// treats as the whole-fix group: every edit) alongside a named-project
// pull request for the mapped edit alone — the mapped edit then appears in
// both PRs. PRProjects must instead collapse the whole fix to the
// single legacy group so exactly one PR opens, carrying every edit.
func TestService_PRProjects_CollapsesWhenAnEditIsUnmapped(t *testing.T) {
	svc := fixes.New(fixes.Deps{
		Repo:   &fakeRepo{},
		NewUoW: (&fakeRepo{}).uowFactory,
		Clock:  fixedClock{},
		// Only "core" is mapped; the finance edit's path matches no entry and
		// falls to the legacy "" key.
		ProjectRepoPaths: map[string]string{"core": "services/core"},
	})

	incomplete := fix.View{Edits: []fix.FileEdit{
		{Path: "services/core/models/a.sql", MemberNodeIDs: []string{"model.core.a"}},
		{Path: "services/finance/models/b.sql", MemberNodeIDs: []string{"model.finance.b"}},
	}}
	require.Equal(t, []string{""}, svc.PRProjects(incomplete),
		"an unmapped edit must collapse the whole fix to the legacy group, never split into overlapping PRs")
}

// TestService_Record_EmitsPROpenedAtomically verifies that Record writes an
// outbox entry with StreamName == streams.FixPrOpenedV1, whose payload
// carries the view's ResolvedNodeIDs (legacy "" group = the whole fixed set),
// and commits the unit of work.
func TestService_Record_EmitsPROpenedAtomically(t *testing.T) {
	repo := &fakeRepo{
		view: fix.View{
			ID:              "p1",
			ReleaseID:       "r-1",
			NodeID:          "model.p.orders_d",
			ResolvedNodeIDs: []string{"model.p.orders_d", "model.p.orders_e"},
			Attempt:         1,
		},
		recordPRCASHit: true,
	}
	svc := fixes.New(fixes.Deps{
		Repo:   repo,
		NewUoW: repo.uowFactory,
		Clock:  fixedClock{},
	})
	err := svc.Record(context.Background(), fixes.RecordInput{
		FixID:    "p1",
		PrURL:    "u",
		PrNumber: 7,
		OpenedBy: "dev|local",
	})
	require.NoError(t, err)
	require.NotNil(t, repo.lastOutbox, "expected an outbox entry to be created")
	require.Equal(t, streams.FixPrOpenedV1, repo.lastOutbox.StreamName)
	require.Equal(t, event.AggregateType, repo.lastOutbox.AggregateType, "the service's events share one outbox aggregate type")
	require.True(t, repo.committed, "expected the unit of work to be committed")
	require.Equal(t, fixedClock{}.Now(), repo.lastRecordPR.openedAt,
		"an unset OpenedAt must fall back to the service clock, as the normal client-side flow relies on")

	var payload serialization.PROpenedDTO
	require.NoError(t, json.Unmarshal(repo.lastOutbox.Payload, &payload))
	require.Equal(t, repo.view.ResolvedNodeIDs, payload.ResolvedNodeIDs,
		"the pr_opened payload must carry the view's ResolvedNodeIDs")
	require.Empty(t, payload.Project, "a legacy record carries no project")
}

// TestService_Record_PROpenedPerProjectResolvedSet verifies a per-project
// Record: the pr_opened payload carries the requested project, its
// resolved_node_ids are narrowed to the members that project's edits address
// (not the whole fixed set), and the outbox entry id derives from the
// three-arg (release, attempt, project) event id.
func TestService_Record_PROpenedPerProjectResolvedSet(t *testing.T) {
	repo := &fakeRepo{
		view: fix.View{
			ID:              "p1",
			ReleaseID:       "r-1",
			NodeID:          "model.core.a",
			ResolvedNodeIDs: []string{"model.core.a", "model.finance.b"},
			NodeOutcomes: map[string]fix.NodeOutcome{
				"model.core.a":    {Status: fix.StatusProposed},
				"model.finance.b": {Status: fix.StatusProposed},
			},
			Attempt: 1,
			Edits: []fix.FileEdit{
				{Path: "services/core/models/a.sql", MemberNodeIDs: []string{"model.core.a"}},
				{Path: "services/finance/models/b.sql", MemberNodeIDs: []string{"model.finance.b"}},
			},
		},
		recordPRCASHit: true,
	}
	svc := fixes.New(fixes.Deps{
		Repo:             repo,
		NewUoW:           repo.uowFactory,
		Clock:            fixedClock{},
		ProjectRepoPaths: map[string]string{"core": "services/core", "finance": "services/finance"},
	})

	require.NoError(t, svc.Record(context.Background(), fixes.RecordInput{
		FixID: "p1", Project: "core", PrURL: "u", PrNumber: 7, OpenedBy: "dev|local",
	}))

	require.Equal(t, "core", repo.lastRecordPR.project, "Record must thread the project to RecordPR")
	require.NotNil(t, repo.lastOutbox)
	require.Equal(t, streams.FixPrOpenedV1, repo.lastOutbox.StreamName)

	var payload serialization.PROpenedDTO
	require.NoError(t, json.Unmarshal(repo.lastOutbox.Payload, &payload))
	require.Equal(t, "core", payload.Project)
	require.Equal(t, []string{"model.core.a"}, payload.ResolvedNodeIDs,
		"a per-project pr_opened resolves only the members that project's edits address")

	wantID := uuid.NewSHA1(uuid.NameSpaceOID, []byte(event.PROpenedEventID("r-1", 1, "core").String()))
	require.Equal(t, wantID, repo.lastOutbox.ID,
		"the outbox entry id must derive from the three-arg per-project event id")
}

// TestService_Record_PROpenedNamesOnlyTheFixedNodes covers the mixed batch: one
// attempt addressed two failing nodes, repaired one and skipped the other, and
// the pull request therefore carries a fix for one of them only. Naming both
// would attach the PR to a rejection it does nothing about — catalog
// records it against every node the event names.
func TestService_Record_PROpenedNamesOnlyTheFixedNodes(t *testing.T) {
	repo := &fakeRepo{
		view: fix.View{
			ID:              "p1",
			ReleaseID:       "r-1",
			NodeID:          "model.p.orders_d",
			ResolvedNodeIDs: []string{"model.p.orders_d", "model.p.orders_e"},
			NodeOutcomes: map[string]fix.NodeOutcome{
				"model.p.orders_d": {Status: fix.StatusProposed},
				"model.p.orders_e": {Status: fix.StatusSkipped, Reason: "no source to fix"},
			},
			Attempt: 1,
		},
		recordPRCASHit: true,
	}
	svc := fixes.New(fixes.Deps{Repo: repo, NewUoW: repo.uowFactory, Clock: fixedClock{}})

	require.NoError(t, svc.Record(context.Background(), fixes.RecordInput{
		FixID: "p1", PrURL: "u", PrNumber: 7, OpenedBy: "dev|local",
	}))

	var payload serialization.PROpenedDTO
	require.NoError(t, json.Unmarshal(repo.lastOutbox.Payload, &payload))
	require.Equal(t, []string{"model.p.orders_d"}, payload.ResolvedNodeIDs,
		"the skipped node's rejection has no fix, so the PR must not be attached to it")
}

// TestService_RecordOutcome_PRClosedNamesOnlyTheFixedNodes is the same
// invariant on the closing half: with the caller passing an empty resolved
// subset (the reconciler's outcome-mirror pass leaves it empty; only the
// amend-compare pass fills it), the PR's outcome falls back to the same
// per-project resolved set the pr_opened event used, so the two agree exactly.
func TestService_RecordOutcome_PRClosedNamesOnlyTheFixedNodes(t *testing.T) {
	repo := &fakeRepo{
		view: fix.View{
			ID: "p1", ReleaseID: "r-1", NodeID: "model.p.orders_d",
			ResolvedNodeIDs: []string{"model.p.orders_d", "model.p.orders_e"},
			NodeOutcomes: map[string]fix.NodeOutcome{
				"model.p.orders_d": {Status: fix.StatusProposed},
				"model.p.orders_e": {Status: fix.StatusFailed, Reason: "nothing to verify"},
			},
			Attempt: 1, PrURL: "http://gh/pull/7", PrNumber: 7,
		},
		outcomeCASHit: true,
	}
	svc := fixes.New(fixes.Deps{Repo: repo, NewUoW: repo.uowFactory, Clock: fixedClock{}})

	closedAt, _ := time.Parse(time.RFC3339, "2026-07-03T10:00:00Z")
	require.NoError(t, svc.RecordOutcome(context.Background(), "p1", "", fix.PROutcomeMerged, closedAt, nil, nil))

	var payload serialization.PRClosedDTO
	require.NoError(t, json.Unmarshal(repo.lastOutbox.Payload, &payload))
	require.Equal(t, []string{"model.p.orders_d"}, payload.ResolvedNodeIDs)
}

// TestService_RecordOutcome_PRClosedCarriesProjectAndEdits verifies a
// per-project close: the pr_closed payload carries the project, the outcome,
// and — passed by the caller — the amended edits and resolved subset verbatim,
// with the outbox id derived from the three-arg event id.
func TestService_RecordOutcome_PRClosedCarriesProjectAndEdits(t *testing.T) {
	repo := &fakeRepo{
		view: fix.View{
			ID: "p1", ReleaseID: "r-1", NodeID: "model.core.a",
			ResolvedNodeIDs: []string{"model.core.a", "model.finance.b"},
			Attempt:         1, PrURL: "http://gh/pull/7", PrNumber: 7,
			Edits: []fix.FileEdit{
				{Path: "services/core/models/a.sql", MemberNodeIDs: []string{"model.core.a"}},
			},
		},
		outcomeCASHit: true,
	}
	svc := fixes.New(fixes.Deps{
		Repo:             repo,
		NewUoW:           repo.uowFactory,
		Clock:            fixedClock{},
		ProjectRepoPaths: map[string]string{"core": "services/core", "finance": "services/finance"},
	})

	closedAt, _ := time.Parse(time.RFC3339, "2026-07-03T10:00:00Z")
	edits := []event.ClosedEdit{
		{Path: "services/core/models/a.sql", TargetNodeID: "model.core.a", Amended: true, Diff: "@@ -1 +1 @@"},
	}
	resolved := []string{"model.core.a"}
	require.NoError(t, svc.RecordOutcome(context.Background(), "p1", "core", fix.PROutcomeMerged, closedAt, edits, resolved))

	require.Equal(t, "core", repo.lastOutcomeProject, "RecordOutcome must thread the project to RecordPROutcome")
	require.Equal(t, streams.FixPrClosedV1, repo.lastOutbox.StreamName)

	var payload serialization.PRClosedDTO
	require.NoError(t, json.Unmarshal(repo.lastOutbox.Payload, &payload))
	require.Equal(t, "core", payload.Project)
	require.Equal(t, "merged", payload.Outcome)
	require.Equal(t, resolved, payload.ResolvedNodeIDs, "a caller-supplied resolved subset is carried verbatim")
	require.Len(t, payload.Edits, 1)
	require.Equal(t, serialization.ClosedEditDTO(edits[0]), payload.Edits[0])

	wantID := uuid.NewSHA1(uuid.NameSpaceOID, []byte(event.PRClosedEventID("r-1", 1, "core").String()))
	require.Equal(t, wantID, repo.lastOutbox.ID,
		"the outbox entry id must derive from the three-arg per-project event id")
}

// TestService_Record_UsesProvidedOpenedAt verifies that a non-zero
// RecordInput.OpenedAt — as the opening sweep supplies from GitHub's own
// created_at when recovering a stranded PR — is written through to
// RecordPR instead of the current clock time.
func TestService_Record_UsesProvidedOpenedAt(t *testing.T) {
	repo := &fakeRepo{
		view: fix.View{
			ID:        "p1",
			ReleaseID: "r-1",
			NodeID:    "model.p.orders_d",
			Attempt:   1,
		},
		recordPRCASHit: true,
	}
	svc := fixes.New(fixes.Deps{
		Repo:   repo,
		NewUoW: repo.uowFactory,
		Clock:  fixedClock{},
	})
	githubCreatedAt := fixedClock{}.Now().Add(-90 * time.Minute)
	err := svc.Record(context.Background(), fixes.RecordInput{
		FixID:    "p1",
		PrURL:    "u",
		PrNumber: 7,
		OpenedBy: "dev|local",
		OpenedAt: githubCreatedAt,
	})
	require.NoError(t, err)
	require.Equal(t, githubCreatedAt, repo.lastRecordPR.openedAt,
		"a provided OpenedAt must be used verbatim, not overridden by the current clock time")
}

// TestService_Record_NoEventWhenCASMisses verifies a CAS miss (row no longer
// 'opening', e.g. already recorded by the reconciler's opening sweep or the
// ui route racing to record the same claim) produces no outbox entry,
// no commit, and no error — Record is idempotent under that race.
func TestService_Record_NoEventWhenCASMisses(t *testing.T) {
	repo := &fakeRepo{
		view:           fix.View{ID: "p1", ReleaseID: "r-1", NodeID: "model.p.orders_d", Attempt: 1},
		recordPRCASHit: false,
	}
	svc := fixes.New(fixes.Deps{Repo: repo, NewUoW: repo.uowFactory, Clock: fixedClock{}})

	err := svc.Record(context.Background(), fixes.RecordInput{
		FixID:    "p1",
		PrURL:    "u",
		PrNumber: 7,
		OpenedBy: "dev|local",
	})
	require.NoError(t, err, "a CAS miss is an idempotent no-op, not an error")
	require.Nil(t, repo.lastOutbox, "no event may be emitted when the CAS misses")
	require.False(t, repo.committed, "the transaction must not commit when nothing was written")
}

// TestService_RecordOutcome_EmitsPRClosedAtomically verifies that a fired CAS
// writes a fix.pr_closed:v1 outbox entry and commits the unit of work.
func TestService_RecordOutcome_EmitsPRClosedAtomically(t *testing.T) {
	repo := &fakeRepo{
		view: fix.View{
			ID: "p1", ReleaseID: "r-1", NodeID: "model.p.orders_d",
			Attempt: 1, PrURL: "http://gh/pull/7", PrNumber: 7,
		},
		outcomeCASHit: true,
	}
	svc := fixes.New(fixes.Deps{Repo: repo, NewUoW: repo.uowFactory, Clock: fixedClock{}})

	closedAt, _ := time.Parse(time.RFC3339, "2026-07-03T10:00:00Z")
	require.NoError(t, svc.RecordOutcome(context.Background(), "p1", "", fix.PROutcomeMerged, closedAt, nil, nil))

	require.Equal(t, fix.PROutcomeMerged, repo.lastOutcome)
	require.Equal(t, closedAt, repo.lastClosedAt)
	require.NotNil(t, repo.lastOutbox, "expected a pr_closed outbox entry")
	require.Equal(t, streams.FixPrClosedV1, repo.lastOutbox.StreamName)
	require.Equal(t, event.AggregateType, repo.lastOutbox.AggregateType, "the service's events share one outbox aggregate type")
	require.True(t, repo.committed, "expected the unit of work to be committed")
}

// TestService_RecordOutcome_NoEventWhenAlreadyTerminal verifies a CAS miss
// (row no longer 'open') produces no outbox entry and no error.
func TestService_RecordOutcome_NoEventWhenAlreadyTerminal(t *testing.T) {
	repo := &fakeRepo{
		view:          fix.View{ID: "p1", ReleaseID: "r-1", NodeID: "n", Attempt: 1},
		outcomeCASHit: false,
	}
	svc := fixes.New(fixes.Deps{Repo: repo, NewUoW: repo.uowFactory, Clock: fixedClock{}})

	err := svc.RecordOutcome(context.Background(), "p1", "", fix.PROutcomeRejected, time.Now(), nil, nil)
	require.NoError(t, err, "a CAS miss is an idempotent no-op, not an error")
	require.Nil(t, repo.lastOutbox, "no event may be emitted when the CAS misses")
}

// TestService_FailStuckClaim_PassesThroughIDProjectAndObservedClaimedAt verifies
// FailStuckClaim delegates to the repository's CAS variant with the exact id,
// project, and observedClaimedAt it was given, and returns the repository's hit
// value unchanged. Both callers rely on that pass-through to distinguish
// "released" from "a fresher claim raced ahead of me": the reconciler's opening
// sweep, and the gRPC FailPullRequest handler on the ui PR-creation route's own
// failure callback.
func TestService_FailStuckClaim_PassesThroughIDProjectAndObservedClaimedAt(t *testing.T) {
	repo := &fakeRepo{failStuckHit: true}
	svc := fixes.New(fixes.Deps{Repo: repo, NewUoW: repo.uowFactory, Clock: fixedClock{}})

	observed := fixedClock{}.Now().Add(-time.Hour)
	hit, err := svc.FailStuckClaim(context.Background(), "p1", "core", observed)
	require.NoError(t, err)
	require.True(t, hit)
	require.Equal(t, "p1", repo.lastFailStuckID)
	require.Equal(t, "core", repo.lastFailStuckProject)
	require.Equal(t, observed, repo.lastFailStuckClaimedAt)

	// A repository-reported miss (re-claimed since observed) passes through
	// as false, not an error.
	repo.failStuckHit = false
	hit, err = svc.FailStuckClaim(context.Background(), "p1", "core", observed)
	require.NoError(t, err)
	require.False(t, hit, "a CAS miss must surface as hit=false, never an error")
}
