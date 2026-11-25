package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/carolsimone/continuo/release-controller/domain/repository"
)

// ServiceProdRepository is the Postgres-backed implementation of
// repository.ServiceProdRepository. It owns the per-project production pointer
// rows in the service_prod table.
type ServiceProdRepository struct{ q Queryer }

// NewServiceProdRepository constructs a ServiceProdRepository bound to the
// given Queryer. Pass *sqlx.DB for autocommit operations or *sqlx.Tx for
// transactional writes.
func NewServiceProdRepository(q Queryer) *ServiceProdRepository {
	return &ServiceProdRepository{q: q}
}

var _ repository.ServiceProdRepository = (*ServiceProdRepository)(nil)

// serviceProdRow is the scan target for SELECT queries against the service_prod table.
type serviceProdRow struct {
	ProjectName   string    `db:"project_name"`
	ReleaseID     string    `db:"release_id"`
	ManifestS3Key string    `db:"manifest_s3_key"`
	ImageTag      string    `db:"image_tag"`
	ManifestKind  string    `db:"manifest_kind"`
	UpdatedAt     time.Time `db:"updated_at"`
}

// List returns all per-project production pointers ordered by project name.
func (r *ServiceProdRepository) List(ctx context.Context) ([]*release.ServiceProd, error) {
	rows, err := r.q.QueryxContext(ctx,
		`SELECT project_name, release_id, manifest_s3_key, image_tag, manifest_kind, updated_at
		   FROM service_prod
		  ORDER BY project_name`)
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
		out = append(out, release.NewServiceProd(row.ProjectName, row.ReleaseID, row.ManifestS3Key, row.ImageTag, release.ManifestKind(row.ManifestKind), row.UpdatedAt))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate service_prod rows: %w", err)
	}
	return out, nil
}

// Get returns the production pointer for the named project. When no row exists
// for that project, (nil, nil) is returned — not an error.
func (r *ServiceProdRepository) Get(ctx context.Context, projectName string) (*release.ServiceProd, error) {
	var row serviceProdRow
	err := r.q.GetContext(ctx, &row,
		`SELECT project_name, release_id, manifest_s3_key, image_tag, manifest_kind, updated_at
		   FROM service_prod
		  WHERE project_name = $1`, projectName)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get service_prod %q: %w", projectName, err)
	}
	return release.NewServiceProd(row.ProjectName, row.ReleaseID, row.ManifestS3Key, row.ImageTag, release.ManifestKind(row.ManifestKind), row.UpdatedAt), nil
}

// Upsert inserts or updates the production pointer for the project recorded in
// sp. If a row already exists for sp.ProjectName(), it is fully replaced.
func (r *ServiceProdRepository) Upsert(ctx context.Context, sp *release.ServiceProd) error {
	_, err := r.q.ExecContext(ctx,
		`INSERT INTO service_prod (project_name, release_id, manifest_s3_key, image_tag, manifest_kind, updated_at)
		 VALUES ($1, $2, $3, $4, $5, $6)
		 ON CONFLICT (project_name) DO UPDATE SET
		   release_id      = EXCLUDED.release_id,
		   manifest_s3_key = EXCLUDED.manifest_s3_key,
		   image_tag       = EXCLUDED.image_tag,
		   manifest_kind   = EXCLUDED.manifest_kind,
		   updated_at      = EXCLUDED.updated_at`,
		sp.ProjectName(), sp.ReleaseID(), sp.ManifestS3Key(), sp.ImageTag(), string(sp.ManifestKind()), sp.UpdatedAt())
	if err != nil {
		return fmt.Errorf("upsert service_prod %q: %w", sp.ProjectName(), err)
	}
	return nil
}

// ProjectsWithKind returns the names of the projects whose pointer has the
// given manifest kind, sorted ascending. When none has it the list is empty.
func (r *ServiceProdRepository) ProjectsWithKind(ctx context.Context, kind release.ManifestKind) ([]string, error) {
	rows, err := r.q.QueryxContext(ctx,
		`SELECT project_name
		   FROM service_prod
		  WHERE manifest_kind = $1
		  ORDER BY project_name`, string(kind))
	if err != nil {
		return nil, fmt.Errorf("list service_prod of kind %q: %w", kind, err)
	}
	defer func() { _ = rows.Close() }()

	names := []string{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("scan service_prod name: %w", err)
		}
		names = append(names, name)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate service_prod of kind %q: %w", kind, err)
	}
	return names, nil
}
