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

func newContractFile(t *testing.T, a *fakeArchive, l *fakeLocator) *contract.ContractFile {
	t.Helper()
	return contract.NewContractFile(a, l, discardLogger())
}

func gatherContractFile(t *testing.T, a *fakeArchive, l *fakeLocator, f remediation.Failure) step.ContextOutput {
	t.Helper()
	out, err := newContractFile(t, a, l).Gather(context.Background(), step.ContextInput{Failure: f, Evidence: remediation.NewEvidence()})
	require.NoError(t, err)
	return out
}

func foundLocator() *fakeLocator {
	return &fakeLocator{located: map[string]ports.Located{"fx.rates": theLocated}}
}

func TestContractFile_MetaNamesTheStepAndWhatItProvides(t *testing.T) {
	m := contract.NewContractFile(nil, nil, nil).Meta()
	assert.Equal(t, "contract_file", string(m.Name))
	assert.Equal(t, step.KindContext, m.Kind)
	assert.NotEmpty(t, m.Description)
	assert.Equal(t, []remediation.EvidenceKey{remediation.KeyFailure}, m.Needs)
	assert.Equal(t, []remediation.EvidenceKey{remediation.KeyContractFile}, m.Provides)
}

func TestContractFile_KeepsThePathsAndTheYAMLAndRemovesTheCheckout(t *testing.T) {
	a, l := &fakeArchive{t: t}, foundLocator()

	out := gatherContractFile(t, a, l, failure(nil))

	require.False(t, out.Declined)
	assert.Equal(t, map[remediation.EvidenceKey]any{
		remediation.KeyContractFile: remediation.ContractFile{
			Schema:      "fx",
			Table:       "rates",
			RepoRoot:    "svc",
			ContractDir: "svc/contracts",
			YAMLPath:    "svc/contracts/fx.yaml",
			YAMLText:    "nodes:\n  - table: rates\n",
		},
	}, out.Evidence)
	assert.Equal(t, []fetchCall{{repoName, commit}}, a.fetches)
	require.Len(t, a.dirs, 1)
	assert.Equal(t, []locateCall{{a.dirs[0], "fx", "rates"}}, l.calls)
	a.requireNoCheckoutLeft()
}

func TestContractFile_TheSchemaAndTableAreTheLastTwoSegmentsOfTheNodeID(t *testing.T) {
	a, l := &fakeArchive{t: t}, foundLocator()

	gatherContractFile(t, a, l, failure(func(f *remediation.Failure) { f.Target.NodeID = "a.b.fx.rates" }))

	require.Len(t, l.calls, 1)
	assert.Equal(t, "fx", l.calls[0].schema)
	assert.Equal(t, "rates", l.calls[0].table)
}

func TestContractFile_DeclinesWithTheLanesTextBeforeFetchingAnything(t *testing.T) {
	cases := []struct {
		name string
		mod  func(*remediation.Failure)
		want string
	}{
		{
			name: "a node id with no dot",
			mod:  func(f *remediation.Failure) { f.Target.NodeID = "rates" },
			want: `node id "rates" does not name a schema and a table, so the contract file declaring it cannot be found`,
		},
		{
			name: "a node id with an empty table",
			mod:  func(f *remediation.Failure) { f.Target.NodeID = "fx." },
			want: `node id "fx." does not name a schema and a table, so the contract file declaring it cannot be found`,
		},
		{
			name: "no service",
			mod:  func(f *remediation.Failure) { f.Target.Service = "" },
			want: "the trigger names no service, so the fix has no release to be verified under",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a, l := &fakeArchive{t: t}, foundLocator()

			out := gatherContractFile(t, a, l, failure(tc.mod))

			assert.True(t, out.Declined)
			assert.Equal(t, tc.want, out.Decline)
			assert.Nil(t, out.Evidence)
			assert.Empty(t, a.fetches)
			assert.Empty(t, l.calls)
		})
	}
}

func TestContractFile_DeclinesARepositoryThatIsNotAvailable(t *testing.T) {
	a := &fakeArchive{t: t, err: fmt.Errorf("github: %w", ports.ErrSourceNotFound)}

	out := gatherContractFile(t, a, foundLocator(), failure(nil))

	assert.True(t, out.Declined)
	assert.Equal(t, "repository org/repo at commit abc123 is not available", out.Decline)
}

func TestContractFile_ReturnsATransientFetchError(t *testing.T) {
	a := &fakeArchive{t: t, err: errBoom}

	_, err := newContractFile(t, a, foundLocator()).Gather(context.Background(),
		step.ContextInput{Failure: failure(nil), Evidence: remediation.NewEvidence()})

	require.ErrorIs(t, err, errBoom)
	assert.EqualError(t, err, "fetch repo org/repo@abc123: boom")
}

func TestContractFile_DeclinesANodeNoContractDeclaresOrSeveralDo(t *testing.T) {
	for _, locateErr := range []error{ports.ErrNodeNotDeclared, ports.ErrAmbiguousDeclaration} {
		t.Run(locateErr.Error(), func(t *testing.T) {
			wrapped := fmt.Errorf("%w: fx.rates", locateErr)
			a := &fakeArchive{t: t}
			l := &fakeLocator{errs: map[string]error{"fx.rates": wrapped}}

			out := gatherContractFile(t, a, l, failure(nil))

			assert.True(t, out.Declined)
			assert.Equal(t, wrapped.Error(), out.Decline)
			a.requireNoCheckoutLeft()
		})
	}
}

func TestContractFile_ReturnsAnyOtherLocateErrorAndRemovesTheCheckout(t *testing.T) {
	a := &fakeArchive{t: t}
	l := &fakeLocator{errs: map[string]error{"fx.rates": errBoom}}

	_, err := newContractFile(t, a, l).Gather(context.Background(),
		step.ContextInput{Failure: failure(nil), Evidence: remediation.NewEvidence()})

	require.ErrorIs(t, err, errBoom)
	assert.EqualError(t, err, "locate contract for py.fx.rates: boom")
	a.requireNoCheckoutLeft()
}
