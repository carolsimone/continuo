//go:build integration

package postgres_test

import (
	"context"
	"strings"
	"testing"
	"time"

	pkgmodel "github.com/carolsimone/continuo/pkg/domain/model"
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

	svcA := release.NewServiceProd("svc-a", "sha-1", "s3://bucket/svc-a/v1.json", "image-a:sha-1", now)
	svcB := release.NewServiceProd("svc-b", "sha-2", "s3://bucket/svc-b/v1.json", "image-b:sha-2", now)

	require.NoError(t, repo.Upsert(ctx, svcA))
	require.NoError(t, repo.Upsert(ctx, svcB))

	// List returns both rows.
	all, err := repo.List(ctx)
	require.NoError(t, err)
	assert.Len(t, all, 2)

	// Re-upserting svc-a with new values is idempotent on the primary key
	// and updates the row; the list length stays at 2.
	updatedA := release.NewServiceProd("svc-a", "sha-99", "s3://bucket/svc-a/v2.json", "image-a:sha-99", now.Add(time.Hour))
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

	// Get for a missing service returns (nil, nil) — not an error.
	missing, err := repo.Get(ctx, "svc-does-not-exist")
	require.NoError(t, err)
	assert.Nil(t, missing)
}

// runtimeRefFixture builds a complete reference distinguished by seed, so tests
// can tell one service's artifact from another's.
func runtimeRefFixture(seed string) pkgmodel.RuntimeManifestRef {
	return pkgmodel.RuntimeManifestRef{
		RuntimeManifestURI:                "s3://bucket/" + seed + "/manifest.msgpack",
		RuntimeManifestSHA256:             seed + strings.Repeat("0", 64-len(seed)),
		RuntimeManifestDBTVersion:         "1.12.0b1",
		RuntimeManifestParseContextSHA256: seed + strings.Repeat("1", 64-len(seed)),
	}
}

func TestServiceProdRepository_RuntimeManifestRoundTrip(t *testing.T) {
	db := openTestDB(t)
	repo := postgres.NewServiceProdRepository(db)
	ctx := context.Background()
	now := time.Unix(1_000_000, 0).UTC()

	ref := runtimeRefFixture("abc")
	require.NoError(t, repo.Upsert(ctx, release.NewServiceProdWithRuntime(
		"svc-rt", "sha-1", "s3://bucket/svc-rt/v1.json", "image:sha-1", ref, now)))

	got, err := repo.Get(ctx, "svc-rt")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, ref, got.RuntimeManifest(), "Get must reconstitute all four runtime columns")

	all, err := repo.List(ctx)
	require.NoError(t, err)
	var listed *release.ServiceProd
	for _, sp := range all {
		if sp.ServiceName() == "svc-rt" {
			listed = sp
		}
	}
	require.NotNil(t, listed)
	assert.Equal(t, ref, listed.RuntimeManifest(), "List must reconstitute all four runtime columns")
}

func TestServiceProdRepository_LegacyRowHasNullRuntimeColumns(t *testing.T) {
	// A pointer written without a runtime manifest must store SQL NULLs, not
	// empty strings: the all-or-none CHECK treats '' as present and would reject
	// the row, and a reader must see "no manifest" rather than a partial one.
	db := openTestDB(t)
	repo := postgres.NewServiceProdRepository(db)
	ctx := context.Background()
	now := time.Unix(1_000_000, 0).UTC()

	require.NoError(t, repo.Upsert(ctx, release.NewServiceProd(
		"svc-legacy", "sha-1", "s3://bucket/svc-legacy/v1.json", "image:sha-1", now)))

	var nullCount int
	require.NoError(t, db.GetContext(ctx, &nullCount,
		`SELECT count(*) FROM service_prod
		  WHERE service_name = $1
		    AND runtime_manifest_uri IS NULL
		    AND runtime_manifest_sha256 IS NULL
		    AND runtime_manifest_dbt_version IS NULL
		    AND runtime_manifest_parse_context_sha256 IS NULL`, "svc-legacy"))
	assert.Equal(t, 1, nullCount, "legacy pointer must persist as NULL runtime columns")

	got, err := repo.Get(ctx, "svc-legacy")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, pkgmodel.RuntimeManifestRef{}, got.RuntimeManifest())
}

func TestServiceProdRepository_UpsertReplacesRuntimeManifest(t *testing.T) {
	db := openTestDB(t)
	repo := postgres.NewServiceProdRepository(db)
	ctx := context.Background()
	now := time.Unix(1_000_000, 0).UTC()

	first := runtimeRefFixture("abc")
	require.NoError(t, repo.Upsert(ctx, release.NewServiceProdWithRuntime(
		"svc-up", "sha-1", "s3://bucket/svc-up/v1.json", "image:sha-1", first, now)))

	// A later release for the same service replaces the pinned artifact.
	second := runtimeRefFixture("def")
	require.NoError(t, repo.Upsert(ctx, release.NewServiceProdWithRuntime(
		"svc-up", "sha-2", "s3://bucket/svc-up/v2.json", "image:sha-2", second, now.Add(time.Hour))))

	got, err := repo.Get(ctx, "svc-up")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, second, got.RuntimeManifest())

	// Re-pointing at a release with no runtime manifest must clear the columns
	// back to NULL rather than leaving the previous artifact pinned.
	require.NoError(t, repo.Upsert(ctx, release.NewServiceProd(
		"svc-up", "sha-3", "s3://bucket/svc-up/v3.json", "image:sha-3", now.Add(2*time.Hour))))

	got, err = repo.Get(ctx, "svc-up")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, pkgmodel.RuntimeManifestRef{}, got.RuntimeManifest())
}

func TestServiceProdRepository_RejectsPartialRuntimeManifest(t *testing.T) {
	// The all-or-none CHECK is the database's own guard: even if a caller
	// bypassed the domain, a half-filled reference must not reach a row.
	db := openTestDB(t)
	ctx := context.Background()

	_, err := db.ExecContext(ctx,
		`INSERT INTO service_prod
		   (service_name, release_id, manifest_s3_key, image_tag, updated_at, runtime_manifest_uri)
		 VALUES ('svc-partial', 'sha-1', 's3://bucket/k.json', 'image:sha-1', now(), 's3://bucket/a.msgpack')`)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "service_prod_runtime_manifest_all_or_none")
}
