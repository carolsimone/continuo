package handlers_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/carolsimone/continuo/release-controller/service/handlers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// failingServiceProdRepo is a ServiceProdRepository whose ServicesWithKind
// fails with err; it embeds the store-backed fake for every other method.
type failingServiceProdRepo struct {
	*fakeServiceProdRepo
	err error
}

func (f failingServiceProdRepo) ServicesWithKind(context.Context, release.ManifestKind) ([]string, error) {
	return nil, f.err
}

func seedServiceProd(t *testing.T, repo *fakeServiceProdRepo, name string, kind release.ManifestKind) {
	t.Helper()
	sp := release.NewServiceProd(name, "rel-1", "s3://b/"+name+"/manifest.json", "img:1", kind, time.Unix(1_000_000, 0).UTC())
	require.NoError(t, repo.Upsert(context.Background(), sp))
}

// A service still released as kind python stops release-controller from
// starting: the error names it, in the words the release migration uses.
func TestStartup_RefusesWhileAServiceIsStillPython(t *testing.T) {
	repo := &fakeServiceProdRepo{store: newFakeStore()}
	seedServiceProd(t, repo, "service-py", release.ManifestKindPython)
	seedServiceProd(t, repo, "service-dbt", release.ManifestKindDbt)
	seedServiceProd(t, repo, "service-odcs", release.ManifestKindPythonODCS)

	err := handlers.CheckNoServiceOnPythonKind(context.Background(), repo)
	require.Error(t, err)

	var refusal *handlers.PythonKindServicesError
	require.ErrorAs(t, err, &refusal)
	assert.Equal(t, []string{"service-py"}, refusal.Services)
	assert.Equal(t, `these services are still released as kind "python": service-py. `+
		`Release each one as "python-odcs" (docs/contracts.md) on the continuo version you run now, then upgrade.`, err.Error())
}

// Every service of kind python is named, sorted and comma-separated.
func TestStartup_NamesEveryPythonServiceSorted(t *testing.T) {
	repo := &fakeServiceProdRepo{store: newFakeStore()}
	seedServiceProd(t, repo, "service-b", release.ManifestKindPython)
	seedServiceProd(t, repo, "service-a", release.ManifestKindPython)
	seedServiceProd(t, repo, "service-c", release.ManifestKindPython)

	err := handlers.CheckNoServiceOnPythonKind(context.Background(), repo)
	require.Error(t, err)
	assert.Contains(t, err.Error(), `kind "python": service-a, service-b, service-c. Release each one`)
}

func TestStartup_PassesWithNoPythonService(t *testing.T) {
	repo := &fakeServiceProdRepo{store: newFakeStore()}
	require.NoError(t, handlers.CheckNoServiceOnPythonKind(context.Background(), repo), "an empty service_prod passes")

	seedServiceProd(t, repo, "service-dbt", release.ManifestKindDbt)
	seedServiceProd(t, repo, "service-odcs", release.ManifestKindPythonODCS)
	require.NoError(t, handlers.CheckNoServiceOnPythonKind(context.Background(), repo), "dbt and python-odcs services pass")
}

// A read that fails is surfaced as that failure, not as a refusal that names
// no service.
func TestStartup_ReportsAFailedRead(t *testing.T) {
	boom := errors.New("connection refused")
	repo := failingServiceProdRepo{fakeServiceProdRepo: &fakeServiceProdRepo{store: newFakeStore()}, err: boom}

	err := handlers.CheckNoServiceOnPythonKind(context.Background(), repo)
	require.Error(t, err)
	assert.ErrorIs(t, err, boom)
	var refusal *handlers.PythonKindServicesError
	assert.False(t, errors.As(err, &refusal), "a failed read is not the python-kind refusal")
}

// The startup check and release migration V30 tell an operator the same thing:
// the check's message is the migration's RAISE format with the names in place of
// its % placeholder. If they differed, the operator would read contradictory
// advice depending on where the install stopped.
func TestStartup_MessageMatchesTheMigration(t *testing.T) {
	_, thisFile, _, ok := runtime.Caller(0)
	require.True(t, ok, "locate test file")
	repoRoot := filepath.Dir(filepath.Dir(filepath.Dir(filepath.Dir(thisFile))))
	matches, err := filepath.Glob(filepath.Join(repoRoot, "db", "migration", "release", "V30__*.sql"))
	require.NoError(t, err)
	require.Len(t, matches, 1, "exactly one release migration has version 30")
	sql, err := os.ReadFile(matches[0])
	require.NoError(t, err)

	format := (&handlers.PythonKindServicesError{Services: []string{"%"}}).Error()
	assert.Contains(t, string(sql), "RAISE EXCEPTION '"+format+"', names;",
		"release migration V30 must raise the message the startup check returns")
}
