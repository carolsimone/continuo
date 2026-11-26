package handlers_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/carolsimone/continuo/release-controller/service/handlers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// failingServiceProdRepo is a ServiceProdRepository whose ProjectsWithKind
// fails with err; it embeds the store-backed fake for every other method.
type failingServiceProdRepo struct {
	*fakeServiceProdRepo
	err error
}

func (f failingServiceProdRepo) ProjectsWithKind(context.Context, release.ManifestKind) ([]string, error) {
	return nil, f.err
}

func seedServiceProd(t *testing.T, repo *fakeServiceProdRepo, name string, kind release.ManifestKind) {
	t.Helper()
	sp := release.NewServiceProd(name, "rel-1", "s3://b/"+name+"/manifest.json", "img:1", kind, time.Unix(1_000_000, 0).UTC())
	require.NoError(t, repo.Upsert(context.Background(), sp))
}

// A project still released as kind python stops release-controller from
// starting: the error names it.
func TestStartup_RefusesWhileAServiceIsStillPython(t *testing.T) {
	repo := &fakeServiceProdRepo{store: newFakeStore()}
	seedServiceProd(t, repo, "service-py", release.ManifestKindPython)
	seedServiceProd(t, repo, "service-dbt", release.ManifestKindDbt)
	seedServiceProd(t, repo, "service-odcs", release.ManifestKindPythonODCS)

	err := handlers.CheckNoProjectOnPythonKind(context.Background(), repo)
	require.Error(t, err)

	var refusal *handlers.PythonKindProjectsError
	require.ErrorAs(t, err, &refusal)
	assert.Equal(t, []string{"service-py"}, refusal.Projects)
	assert.Equal(t, `these projects are still released as kind "python": service-py. `+
		`Release each one as "python-odcs" (docs/contracts.md) on a continuo version that accepts it, then upgrade.`, err.Error())
}

// Every project of kind python is named, sorted and comma-separated.
func TestStartup_NamesEveryPythonProjectSorted(t *testing.T) {
	repo := &fakeServiceProdRepo{store: newFakeStore()}
	seedServiceProd(t, repo, "service-b", release.ManifestKindPython)
	seedServiceProd(t, repo, "service-a", release.ManifestKindPython)
	seedServiceProd(t, repo, "service-c", release.ManifestKindPython)

	err := handlers.CheckNoProjectOnPythonKind(context.Background(), repo)
	require.Error(t, err)
	assert.Contains(t, err.Error(), `kind "python": service-a, service-b, service-c. Release each one`)
}

func TestStartup_PassesWithNoPythonProject(t *testing.T) {
	repo := &fakeServiceProdRepo{store: newFakeStore()}
	require.NoError(t, handlers.CheckNoProjectOnPythonKind(context.Background(), repo), "an empty service_prod passes")

	seedServiceProd(t, repo, "service-dbt", release.ManifestKindDbt)
	seedServiceProd(t, repo, "service-odcs", release.ManifestKindPythonODCS)
	require.NoError(t, handlers.CheckNoProjectOnPythonKind(context.Background(), repo), "dbt and python-odcs projects pass")
}

// A read that fails is surfaced as that failure, not as a refusal that names
// no project.
func TestStartup_ReportsAFailedRead(t *testing.T) {
	boom := errors.New("connection refused")
	repo := failingServiceProdRepo{fakeServiceProdRepo: &fakeServiceProdRepo{store: newFakeStore()}, err: boom}

	err := handlers.CheckNoProjectOnPythonKind(context.Background(), repo)
	require.Error(t, err)
	assert.ErrorIs(t, err, boom)
	var refusal *handlers.PythonKindProjectsError
	assert.False(t, errors.As(err, &refusal), "a failed read is not the python-kind refusal")
}
