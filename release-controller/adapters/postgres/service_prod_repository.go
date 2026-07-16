package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	pkgmodel "github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/carolsimone/continuo/release-controller/domain/repository"
)

// ServiceProdRepository is the Postgres-backed implementation of
// repository.ServiceProdRepository. It owns the per-service production pointer
// rows in the service_prod table.
type ServiceProdRepository struct{ q Queryer }

// NewServiceProdRepository constructs a ServiceProdRepository bound to the
// given Queryer. Pass *sqlx.DB for autocommit operations or *sqlx.Tx for
// transactional writes.
func NewServiceProdRepository(q Queryer) *ServiceProdRepository {
	return &ServiceProdRepository{q: q}
}

var _ repository.ServiceProdRepository = (*ServiceProdRepository)(nil)

// serviceProdRow is the scan target for SELECT queries against the service_prod
// table. The runtime manifest columns are nullable — a pointer that pins no
// artifact stores NULLs — so they scan through sql.NullString.
type serviceProdRow struct {
	ServiceName                       string         `db:"service_name"`
	ReleaseID                         string         `db:"release_id"`
	ManifestS3Key                     string         `db:"manifest_s3_key"`
	ImageTag                          string         `db:"image_tag"`
	UpdatedAt                         time.Time      `db:"updated_at"`
	RuntimeManifestURI                sql.NullString `db:"runtime_manifest_uri"`
	RuntimeManifestSHA256             sql.NullString `db:"runtime_manifest_sha256"`
	RuntimeManifestDBTVersion         sql.NullString `db:"runtime_manifest_dbt_version"`
	RuntimeManifestParseContextSHA256 sql.NullString `db:"runtime_manifest_parse_context_sha256"`
}

// serviceProdColumns is the column list every SELECT scans, kept in one place so
// the scan target and the queries cannot drift apart.
const serviceProdColumns = `service_name, release_id, manifest_s3_key, image_tag, updated_at,
	   runtime_manifest_uri, runtime_manifest_sha256, runtime_manifest_dbt_version,
	   runtime_manifest_parse_context_sha256`

// toDomain reconstitutes the pointer. NULL runtime columns yield the zero
// reference, which is how a pointer that pins no artifact reads.
func (row serviceProdRow) toDomain() *release.ServiceProd {
	return release.NewServiceProdWithRuntime(
		row.ServiceName, row.ReleaseID, row.ManifestS3Key, row.ImageTag,
		pkgmodel.RuntimeManifestRef{
			RuntimeManifestURI:                row.RuntimeManifestURI.String,
			RuntimeManifestSHA256:             row.RuntimeManifestSHA256.String,
			RuntimeManifestDBTVersion:         row.RuntimeManifestDBTVersion.String,
			RuntimeManifestParseContextSHA256: row.RuntimeManifestParseContextSHA256.String,
		},
		row.UpdatedAt,
	)
}

// nullIfEmpty maps the empty string to SQL NULL, so a pointer pinning no
// artifact stores NULLs rather than empty strings — which the all-or-none CHECK
// would read as a present-but-partial reference.
func nullIfEmpty(s string) sql.NullString {
	return sql.NullString{String: s, Valid: s != ""}
}

// List returns all per-service production pointers ordered by service name.
func (r *ServiceProdRepository) List(ctx context.Context) ([]*release.ServiceProd, error) {
	rows, err := r.q.QueryxContext(ctx,
		`SELECT `+serviceProdColumns+`
		   FROM service_prod
		  ORDER BY service_name`)
	if err != nil {
		return nil, fmt.Errorf("list service_prod: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []*release.ServiceProd
	for rows.Next() {
		var row serviceProdRow
		if err := rows.StructScan(&row); err != nil {
			return nil, fmt.Errorf("scan service_prod row: %w", err)
		}
		out = append(out, row.toDomain())
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate service_prod rows: %w", err)
	}
	return out, nil
}

// Get returns the production pointer for the named service. When no row exists
// for that service, (nil, nil) is returned — not an error.
func (r *ServiceProdRepository) Get(ctx context.Context, serviceName string) (*release.ServiceProd, error) {
	var row serviceProdRow
	err := r.q.GetContext(ctx, &row,
		`SELECT `+serviceProdColumns+`
		   FROM service_prod
		  WHERE service_name = $1`, serviceName)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get service_prod %q: %w", serviceName, err)
	}
	return row.toDomain(), nil
}

// Upsert inserts or updates the production pointer for the service recorded in
// sp. If a row already exists for sp.ServiceName(), it is fully replaced —
// including the runtime manifest columns, so a release that pins no artifact
// clears a previously pinned one rather than leaving it behind.
func (r *ServiceProdRepository) Upsert(ctx context.Context, sp *release.ServiceProd) error {
	rt := sp.RuntimeManifest()
	_, err := r.q.ExecContext(ctx,
		`INSERT INTO service_prod (service_name, release_id, manifest_s3_key, image_tag, updated_at,
		   runtime_manifest_uri, runtime_manifest_sha256, runtime_manifest_dbt_version,
		   runtime_manifest_parse_context_sha256)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		 ON CONFLICT (service_name) DO UPDATE SET
		   release_id      = EXCLUDED.release_id,
		   manifest_s3_key = EXCLUDED.manifest_s3_key,
		   image_tag       = EXCLUDED.image_tag,
		   updated_at      = EXCLUDED.updated_at,
		   runtime_manifest_uri                  = EXCLUDED.runtime_manifest_uri,
		   runtime_manifest_sha256               = EXCLUDED.runtime_manifest_sha256,
		   runtime_manifest_dbt_version          = EXCLUDED.runtime_manifest_dbt_version,
		   runtime_manifest_parse_context_sha256 = EXCLUDED.runtime_manifest_parse_context_sha256`,
		sp.ServiceName(), sp.ReleaseID(), sp.ManifestS3Key(), sp.ImageTag(), sp.UpdatedAt(),
		nullIfEmpty(rt.RuntimeManifestURI), nullIfEmpty(rt.RuntimeManifestSHA256),
		nullIfEmpty(rt.RuntimeManifestDBTVersion), nullIfEmpty(rt.RuntimeManifestParseContextSHA256))
	if err != nil {
		return fmt.Errorf("upsert service_prod %q: %w", sp.ServiceName(), err)
	}
	return nil
}
