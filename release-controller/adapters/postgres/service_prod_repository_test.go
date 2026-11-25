//go:build integration

package postgres_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/carolsimone/continuo/release-controller/adapters/postgres"
	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestServiceProdRepository_UpsertListGet(t *testing.T) {
	db := openTestDB(t)
	repo := postgres.NewServiceProdRepository(db)
	ctx := context.Background()

	now := time.Unix(1_000_000, 0).UTC()

	svcA := release.NewServiceProd("svc-a", "sha-1", "s3://bucket/svc-a/v1.json", "image-a:sha-1", release.ManifestKindDbt, now)
	svcB := release.NewServiceProd("svc-b", "sha-2", "s3://bucket/svc-b/v1.json", "image-b:sha-2", release.ManifestKindDbt, now)

	require.NoError(t, repo.Upsert(ctx, svcA))
	require.NoError(t, repo.Upsert(ctx, svcB))

	// List returns both rows.
	all, err := repo.List(ctx)
	require.NoError(t, err)
	assert.Len(t, all, 2)

	// Re-upserting svc-a with new values is idempotent on the primary key
	// and updates the row; the list length stays at 2.
	updatedA := release.NewServiceProd("svc-a", "sha-99", "s3://bucket/svc-a/v2.json", "image-a:sha-99", release.ManifestKindDbt, now.Add(time.Hour))
	require.NoError(t, repo.Upsert(ctx, updatedA))

	all, err = repo.List(ctx)
	require.NoError(t, err)
	assert.Len(t, all, 2, "re-upsert must not insert a duplicate row")

	// Get returns the updated values for svc-a.
	got, err := repo.Get(ctx, "svc-a")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, "sha-99", got.ReleaseID())
	assert.Equal(t, "s3://bucket/svc-a/v2.json", got.ManifestS3Key())
	assert.Equal(t, "image-a:sha-99", got.ImageTag())

	// Get for a missing project returns (nil, nil) — not an error.
	missing, err := repo.Get(ctx, "svc-does-not-exist")
	require.NoError(t, err)
	assert.Nil(t, missing)

	// manifest_kind survives the Upsert -> Get roundtrip.
	spPy := release.NewServiceProd("svc-py", "rel-1", "s3://b/svc-py/rel-1/odcs-bundle.json",
		"img:1", release.ManifestKindPythonODCS, now)
	require.NoError(t, repo.Upsert(ctx, spPy))
	gotPy, err := repo.Get(ctx, "svc-py")
	require.NoError(t, err)
	require.NotNil(t, gotPy)
	assert.Equal(t, release.ManifestKindPythonODCS, gotPy.ManifestKind())
}

// TestServiceProdRepository_StoresAnOntologyProject proves service_prod accepts
// a row of the kind ontology and returns it, and still refuses a kind no
// release can carry.
func TestServiceProdRepository_StoresAnOntologyProject(t *testing.T) {
	db := openTestDB(t)
	repo := postgres.NewServiceProdRepository(db)
	ctx := context.Background()
	now := time.Unix(1_000_000, 0).UTC()

	require.NoError(t, repo.Upsert(ctx, release.NewServiceProd("company-ontology", "rel-1",
		"s3://b/company-ontology/rel-1/ontology.yaml", "img:1", release.ManifestKindOntology, now)))
	got, err := repo.Get(ctx, "company-ontology")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, release.ManifestKindOntology, got.ManifestKind())

	_, err = db.ExecContext(ctx, `INSERT INTO service_prod (project_name, release_id, manifest_s3_key, image_tag, manifest_kind)
		VALUES ('svc-bogus', 'rel-1', 's3://b/svc-bogus/rel-1/manifest.json', 'img:1', 'bogus')`)
	require.Error(t, err, "the constraint accepts only the kinds a release can carry")
	assert.Contains(t, err.Error(), "service_prod_manifest_kind_check")
}

// TestProjectsWithKind_ListsThePythonProjects writes service_prod rows of its
// own, in reverse name order, and reads them back by kind. It deletes only the rows
// it wrote, so rows other tests left in the table stay.
func TestProjectsWithKind_ListsThePythonProjects(t *testing.T) {
	db := connectTestDB(t)
	// One connection, with index scans off, makes a query that does not sort
	// return the rows in the order they were written: the rows below are
	// written in reverse name order, so only an ORDER BY returns them sorted.
	db.SetMaxOpenConns(1)
	for _, setting := range []string{"enable_indexscan", "enable_indexonlyscan", "enable_bitmapscan"} {
		_, err := db.Exec("SET " + setting + " = off")
		require.NoError(t, err)
	}
	repo := postgres.NewServiceProdRepository(db)
	ctx := context.Background()
	now := time.Unix(1_000_000, 0).UTC()

	const prefix = "kind-test-"
	rows := []struct {
		name string
		kind release.ManifestKind
	}{
		{prefix + "py-c", release.ManifestKindPython},
		{prefix + "dbt", release.ManifestKindDbt},
		{prefix + "py-b", release.ManifestKindPython},
		{prefix + "odcs", release.ManifestKindPythonODCS},
		{prefix + "py-a", release.ManifestKindPython},
	}
	t.Cleanup(func() {
		_, err := db.Exec(`DELETE FROM service_prod WHERE project_name LIKE $1`, prefix+"%")
		require.NoError(t, err)
	})
	for _, r := range rows {
		require.NoError(t, repo.Upsert(ctx, release.NewServiceProd(r.name, "rel-1", "s3://b/"+r.name+"/manifest.json", "img:1", r.kind, now)))
	}

	// named keeps the projects this test wrote, in the order the repository
	// returned them: another test's leftover row of the kind cannot fail it.
	named := func(names []string) []string {
		out := []string{}
		for _, n := range names {
			if strings.HasPrefix(n, prefix) {
				out = append(out, n)
			}
		}
		return out
	}

	python, err := repo.ProjectsWithKind(ctx, release.ManifestKindPython)
	require.NoError(t, err)
	assert.Equal(t, []string{prefix + "py-a", prefix + "py-b", prefix + "py-c"}, named(python))

	odcs, err := repo.ProjectsWithKind(ctx, release.ManifestKindPythonODCS)
	require.NoError(t, err)
	assert.Equal(t, []string{prefix + "odcs"}, named(odcs))

	// A kind that no row of this test holds yields an empty list, not an error.
	_, err = db.Exec(`DELETE FROM service_prod WHERE project_name LIKE $1 AND manifest_kind = 'python'`, prefix+"%")
	require.NoError(t, err)
	python, err = repo.ProjectsWithKind(ctx, release.ManifestKindPython)
	require.NoError(t, err)
	assert.Empty(t, named(python))
}
