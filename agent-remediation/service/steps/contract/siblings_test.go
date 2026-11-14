package contract_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/agent-remediation/service/runner/step"
	"github.com/carolsimone/continuo/agent-remediation/service/steps/contract"
)

func gatherSiblings(t *testing.T, a *fakeArchive, l *fakeLocator, r *fakeReleases, ev remediation.Evidence) (step.ContextOutput, error) {
	t.Helper()
	return contract.NewFailingSiblings(a, l, r, discardLogger()).Gather(context.Background(),
		step.ContextInput{Failure: failure(nil), Evidence: ev})
}

func sameDir(id string) ports.Located {
	return ports.Located{ContractDir: "svc/contracts", YAMLPath: "svc/contracts/" + id + ".yaml"}
}

func TestFailingSiblings_MetaNamesTheStepAndProvidesNothing(t *testing.T) {
	m := contract.NewFailingSiblings(nil, nil, nil, nil).Meta()
	assert.Equal(t, "failing_siblings", string(m.Name))
	assert.Equal(t, step.KindContext, m.Kind)
	assert.NotEmpty(t, m.Description)
	assert.Equal(t, []remediation.EvidenceKey{remediation.KeyFailure, remediation.KeyContractFile}, m.Needs)
	assert.Empty(t, m.Provides)
}

func TestFailingSiblings_ADeclaredSiblingInTheSameDirectoryDeclinesWithTheLanesText(t *testing.T) {
	a := &fakeArchive{t: t}
	l := &fakeLocator{located: map[string]ports.Located{"fx.other": sameDir("other")}}
	r := &fakeReleases{failing: map[string]string{nodeID: "e1", "py.fx.other": "e2"}}

	out, err := gatherSiblings(t, a, l, r, contractFileEvidence())

	require.NoError(t, err)
	assert.True(t, out.Declined)
	assert.Equal(t, "py.fx.other also failed validation in release rel-1 and is declared in the same contract directory (svc/contracts) as py.fx.rates. "+
		"A fix is packaged from that whole directory, so any verification run of it would re-run both nodes "+
		"and be failed by py.fx.other. Fix both nodes together.", out.Decline)
	assert.Empty(t, out.Evidence)
	assert.Equal(t, []string{"rel-1"}, r.releases)
	assert.Equal(t, []fetchCall{{repoName, commit}}, a.fetches)
	a.requireNoCheckoutLeft()
}

func TestFailingSiblings_LooksUpEachOtherNodeInAFreshCheckoutInSortedOrder(t *testing.T) {
	a := &fakeArchive{t: t}
	l := &fakeLocator{located: map[string]ports.Located{"fx.b": sameDir("b"), "fx.c": sameDir("c")}}
	r := &fakeReleases{failing: map[string]string{nodeID: "e0", "py.fx.c": "e", "py.fx.b": "e"}}

	out, err := gatherSiblings(t, a, l, r, contractFileEvidence())

	require.NoError(t, err)
	assert.True(t, out.Declined)
	assert.Contains(t, out.Decline, "py.fx.b also failed")
	require.Len(t, a.dirs, 1)
	assert.Equal(t, []locateCall{{a.dirs[0], "fx", "b"}}, l.calls, "the first sibling in sorted order decides")
}

func TestFailingSiblings_ASiblingInAnotherDirectoryOrNotPlacedDoesNotDecline(t *testing.T) {
	other := ports.Located{ContractDir: "svc/elsewhere"}
	cases := []struct {
		name string
		l    *fakeLocator
		id   string
	}{
		{"another directory", &fakeLocator{located: map[string]ports.Located{"fx.other": other}}, "py.fx.other"},
		{"not declared anywhere", &fakeLocator{}, "py.fx.other"},
		{"declared twice", &fakeLocator{errs: map[string]error{"fx.other": ports.ErrAmbiguousDeclaration}}, "py.fx.other"},
		{"unsearchable", &fakeLocator{errs: map[string]error{"fx.other": errBoom}}, "py.fx.other"},
		{"an id naming no schema and table", &fakeLocator{}, "other"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a := &fakeArchive{t: t}
			r := &fakeReleases{failing: map[string]string{nodeID: "e1", tc.id: "e2"}}

			out, err := gatherSiblings(t, a, tc.l, r, contractFileEvidence())

			require.NoError(t, err)
			assert.False(t, out.Declined)
			assert.Empty(t, out.Decline)
			assert.Empty(t, out.Evidence)
			a.requireNoCheckoutLeft()
		})
	}
}

func TestFailingSiblings_TheNodeBeingFixedIsNotItsOwnSibling(t *testing.T) {
	a := &fakeArchive{t: t}
	l := &fakeLocator{located: map[string]ports.Located{"fx.rates": theLocated}}
	r := &fakeReleases{failing: map[string]string{nodeID: "e1"}}

	out, err := gatherSiblings(t, a, l, r, contractFileEvidence())

	require.NoError(t, err)
	assert.False(t, out.Declined)
	assert.Empty(t, l.calls)
}

func TestFailingSiblings_ReturnsAFailingNodesReadError(t *testing.T) {
	a := &fakeArchive{t: t}

	_, err := gatherSiblings(t, a, &fakeLocator{}, &fakeReleases{err: errBoom}, contractFileEvidence())

	require.ErrorIs(t, err, errBoom)
	assert.EqualError(t, err, "read failing nodes of release rel-1: boom")
	a.requireNoCheckoutLeft()
}

func TestFailingSiblings_AFetchThatFindsNothingDeclinesAndAnyOtherFetchErrorIsReturned(t *testing.T) {
	notFound := &fakeArchive{t: t, err: fmt.Errorf("github: %w", ports.ErrSourceNotFound)}
	out, err := gatherSiblings(t, notFound, &fakeLocator{}, &fakeReleases{}, contractFileEvidence())
	require.NoError(t, err)
	assert.True(t, out.Declined)
	assert.Equal(t, "repository org/repo at commit abc123 is not available", out.Decline)

	broken := &fakeArchive{t: t, err: errBoom}
	_, err = gatherSiblings(t, broken, &fakeLocator{}, &fakeReleases{}, contractFileEvidence())
	assert.EqualError(t, err, "fetch repo org/repo@abc123: boom")
}

func TestFailingSiblings_NeedsTheContractFile(t *testing.T) {
	a := &fakeArchive{t: t}

	_, err := gatherSiblings(t, a, &fakeLocator{}, &fakeReleases{}, remediation.NewEvidence())

	require.Error(t, err)
	assert.Empty(t, a.fetches)
}
