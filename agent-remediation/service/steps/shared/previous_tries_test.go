package shared_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/domain/proposal"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/domain/repository"
	"github.com/carolsimone/continuo/agent-remediation/serialization"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/agent-remediation/service/runner/step"
	"github.com/carolsimone/continuo/agent-remediation/service/steps/shared"
)

type fakeTries struct {
	snaps []remediation.Snapshot
	err   error
	calls [][2]string
}

func (f *fakeTries) ListTriesOfRelease(_ context.Context, tenantID, releaseID string) ([]remediation.Snapshot, error) {
	f.calls = append(f.calls, [2]string{tenantID, releaseID})
	return f.snaps, f.err
}

var _ repository.TryLister = (*fakeTries)(nil)

type fakeProposals struct {
	views   []proposal.View
	err     error
	filters []repository.ProposalFilter
}

func (f *fakeProposals) List(_ context.Context, filter repository.ProposalFilter) ([]proposal.View, error) {
	f.filters = append(f.filters, filter)
	return f.views, f.err
}

var _ repository.AttemptLister = (*fakeProposals)(nil)

// diffObjects answers each stored diff by its uri, and fails any other.
type diffObjects map[string]string

func (d diffObjects) Fetch(_ context.Context, uri string) (string, error) {
	if body, ok := d[uri]; ok {
		return body, nil
	}
	return "", errors.New("no such object")
}

// previousTriesFailure is round 2's second try at model.a.
func previousTriesFailure() remediation.Failure {
	node := remediation.TriggerNode{NodeID: "model.a"}
	return remediation.Failure{
		Trigger: remediation.Trigger{TenantID: "acme", Source: "validation", ReleaseID: "rel-1", Round: 2},
		Target:  node, TargetNodeID: "model.a", Members: []remediation.TriggerNode{node},
		Attempt: 5, Try: 2,
	}
}

// triedSnapshot is one try of the release: a single cluster at node whose last
// pre-flight round left draft, with the gate's findings when there are any.
func triedSnapshot(t *testing.T, round, try, attempt int, node string, draft remediation.Draft, gate ...remediation.Finding) remediation.Snapshot {
	t.Helper()
	preflight, err := serialization.EncodeRecord(serialization.PreflightRecord{Draft: draft})
	require.NoError(t, err)
	steps := []remediation.StepRun{{
		Try: try, Cluster: 1, Step: remediation.StepPreflight, Kind: remediation.KindRunner, Round: 1,
		Status: remediation.StepDone, Output: preflight,
	}}
	if len(gate) > 0 {
		out, err := serialization.EncodeRecord(serialization.CheckRecord{Result: remediation.CheckResult{Check: "verification_run", Findings: gate}, Gate: true})
		require.NoError(t, err)
		steps = append(steps, remediation.StepRun{
			Try: try, Step: "verification_run", Kind: remediation.KindCheck, Round: 1, Status: remediation.StepDone, Output: out, Findings: gate,
		})
	}
	return remediation.Snapshot{
		Remediation: remediation.Remediation{ID: uuid.New(), TenantID: "acme", ReleaseID: "rel-1", Round: round, CurrentTry: try},
		Try:         &remediation.Try{Try: try, Attempt: attempt},
		Clusters:    []remediation.Cluster{{Cluster: 1, TargetNodeID: node, Members: []string{node}, TypeName: "dbt-validation"}},
		Steps:       steps,
	}
}

func editOf(original, content string) remediation.Draft {
	return remediation.Draft{Edits: []remediation.Edit{{Path: "models/a.sql", Original: original, Content: content}}}
}

func gatherPreviousTries(t *testing.T, tries repository.TryLister, attempts repository.AttemptLister, objects ports.EvidenceReader) []remediation.TryRecord {
	t.Helper()
	out, err := shared.NewPreviousTries(tries, attempts, objects, discardLogger()).Gather(context.Background(),
		step.ContextInput{Failure: previousTriesFailure()})
	require.NoError(t, err)
	got, ok := out.Evidence[remediation.KeyPreviousTries].([]remediation.TryRecord)
	require.True(t, ok, "previous_tries holds a []remediation.TryRecord")
	return got
}

func TestPreviousTries_MetaNamesTheStepAndWhatItProvides(t *testing.T) {
	m := shared.NewPreviousTries(nil, nil, nil, nil).Meta()
	assert.Equal(t, "previous_tries", string(m.Name))
	assert.Equal(t, step.KindContext, m.Kind)
	assert.Equal(t, []remediation.EvidenceKey{remediation.KeyFailure}, m.Needs)
	assert.Equal(t, []remediation.EvidenceKey{remediation.KeyPreviousTries}, m.Provides)
}

func TestPreviousTries_IncludesEarlierRoundsAndAttemptsWithNoRemediation(t *testing.T) {
	broken := remediation.Finding{Code: "verification_failed", Message: "model.a: still broken"}
	tries := &fakeTries{snaps: []remediation.Snapshot{
		triedSnapshot(t, 1, 1, 2, "model.a", editOf("select a\n", "select b\n"), broken),
		triedSnapshot(t, 1, 2, 3, "model.z", editOf("select z\n", "select y\n"), broken),
		triedSnapshot(t, 2, 1, 4, "model.a", editOf("select a\n", "select c\n"), broken),
		triedSnapshot(t, 2, 2, 5, "model.a", editOf("select a\n", "select d\n")),
	}}
	attempts := &fakeProposals{views: []proposal.View{
		{
			Attempt: 1, RemediationRound: 1, ResolvedNodeIDs: []string{"model.a", "model.b"}, VerifyError: "unowned attempt still broken",
			Edits: []proposal.FileEdit{{Path: "models/a.sql", DiffURI: "s3://diffs/1"}, {Path: "models/b.sql", DiffURI: "s3://diffs/missing"}, {Path: "models/c.sql"}},
		},
		// The proposal rows of the runner's own tries, read from the journal instead.
		{Attempt: 2, RemediationRound: 1, RemediationID: uuid.NewString(), ResolvedNodeIDs: []string{"model.a"}, VerifyError: "x"},
		{Attempt: 4, RemediationRound: 2, RemediationID: uuid.NewString(), ResolvedNodeIDs: []string{"model.a"}, VerifyError: "x"},
		// An attempt with no remediation at another node.
		{Attempt: 3, RemediationRound: 1, ResolvedNodeIDs: []string{"model.z"}, VerifyError: "other"},
	}}

	got := gatherPreviousTries(t, tries, attempts, diffObjects{"s3://diffs/1": "-a\n+unowned"})

	gate := []remediation.Finding{broken}
	record := func(round, try, attempt int, content string) remediation.TryRecord {
		return remediation.TryRecord{
			Round: round, Try: try, Attempt: attempt,
			Clusters: []remediation.ClusterRecord{{
				Type: "dbt-validation", TargetNodeID: "model.a", Members: []string{"model.a"},
				Edits:  []remediation.EditRecord{{Path: "models/a.sql", Diff: proposal.ComputeUnifiedDiff("select a\n", content, "models/a.sql")}},
				Rounds: [][]remediation.Finding{nil},
			}},
			Gate: gate,
		}
	}
	assert.Equal(t, []remediation.TryRecord{
		{
			Round: 1, Attempt: 1,
			Clusters: []remediation.ClusterRecord{{
				Members: []string{"model.a", "model.b"},
				Edits:   []remediation.EditRecord{{Path: "models/a.sql", Diff: "-a\n+unowned"}},
			}},
			Gate: []remediation.Finding{{Message: "unowned attempt still broken"}},
		},
		record(1, 1, 2, "select b\n"),
		record(2, 1, 4, "select c\n"),
	}, got, "the attempt with no remediation and every earlier try at the node, oldest first; the try being made is left out")
	assert.Equal(t, [][2]string{{"acme", "rel-1"}}, tries.calls)
	assert.Equal(t, []repository.ProposalFilter{{ReleaseID: "rel-1", Source: "validation"}}, attempts.filters)
}

func TestPreviousTries_AnUnreadableTryIsLeftOut(t *testing.T) {
	bad := triedSnapshot(t, 1, 1, 1, "model.a", editOf("a\n", "b\n"))
	bad.Steps[0].Output = []byte("not json")
	good := triedSnapshot(t, 1, 2, 2, "model.a", editOf("a\n", "c\n"))

	got := gatherPreviousTries(t, &fakeTries{snaps: []remediation.Snapshot{bad, good}}, &fakeProposals{}, nil)

	require.Len(t, got, 1)
	assert.Equal(t, 2, got[0].Try)
}

func TestPreviousTries_AHistoryThatCannotBeReadIsAStepError(t *testing.T) {
	ctx := context.Background()
	in := step.ContextInput{Failure: previousTriesFailure()}

	_, err := shared.NewPreviousTries(&fakeTries{err: errors.New("db down")}, &fakeProposals{}, nil, discardLogger()).Gather(ctx, in)
	require.ErrorContains(t, err, "db down")

	_, err = shared.NewPreviousTries(&fakeTries{}, &fakeProposals{err: errors.New("db down")}, nil, discardLogger()).Gather(ctx, in)
	require.ErrorContains(t, err, "db down")
}

func TestPreviousTries_NoReadersGiveNoTries(t *testing.T) {
	out, err := shared.NewPreviousTries(nil, nil, nil, discardLogger()).Gather(context.Background(),
		step.ContextInput{Failure: previousTriesFailure()})
	require.NoError(t, err)
	got, ok := out.Evidence[remediation.KeyPreviousTries].([]remediation.TryRecord)
	require.True(t, ok)
	assert.Empty(t, got)
}

func TestPreviousTries_NoEvidenceReaderShowsAttemptsWithNoRemediationWithoutDiffs(t *testing.T) {
	attempts := &fakeProposals{views: []proposal.View{{
		Attempt: 1, RemediationRound: 1, ResolvedNodeIDs: []string{"model.a"}, VerifyError: "unowned attempt still broken",
		Edits: []proposal.FileEdit{{Path: "models/a.sql", DiffURI: "s3://diffs/1"}},
	}}}

	got := gatherPreviousTries(t, &fakeTries{}, attempts, nil)

	assert.Equal(t, []remediation.TryRecord{{
		Round: 1, Attempt: 1,
		Clusters: []remediation.ClusterRecord{{Members: []string{"model.a"}}},
		Gate:     []remediation.Finding{{Message: "unowned attempt still broken"}},
	}}, got)
}

// TestPreviousTries_LeavesOutAnAttemptTheStartupSweepFailed: an attempt row with
// no remediation that the startup sweep failed was never judged by a gate, so
// the model is not told its change failed. A generating row the sweep failed
// holds the reason as its rationale, a verifying one as its verify error (with
// its edits and diffs kept); both are left out, and another row with no
// remediation is still listed.
func TestPreviousTries_LeavesOutAnAttemptTheStartupSweepFailed(t *testing.T) {
	attempts := &fakeProposals{views: []proposal.View{
		{
			Attempt: 1, RemediationRound: 1, ResolvedNodeIDs: []string{"model.a"}, Status: proposal.StatusFailed,
			Rationale: "the model's rationale", VerifyError: proposal.NoRemediationInFlightReason,
			Edits: []proposal.FileEdit{{Path: "models/a.sql", DiffURI: "s3://diffs/swept"}},
		},
		{
			Attempt: 2, RemediationRound: 1, ResolvedNodeIDs: []string{"model.a"}, Status: proposal.StatusFailed,
			Rationale: proposal.NoRemediationInFlightReason,
		},
		{
			Attempt: 3, RemediationRound: 1, ResolvedNodeIDs: []string{"model.a"}, Status: proposal.StatusFailed,
			Rationale: "the model's rationale", VerifyError: "judged and still broken",
		},
	}}

	got := gatherPreviousTries(t, &fakeTries{}, attempts, diffObjects{"s3://diffs/swept": "-a\n+swept"})

	assert.Equal(t, []remediation.TryRecord{{
		Round: 1, Attempt: 3,
		Clusters: []remediation.ClusterRecord{{Members: []string{"model.a"}}},
		Gate:     []remediation.Finding{{Message: "judged and still broken"}},
	}}, got, "only the row a gate judged is an earlier try")
}
