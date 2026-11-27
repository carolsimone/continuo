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

// ProductionPointerRepository is the Postgres-backed implementation of
// repository.ProductionPointerRepository. It owns the per-project production pointer
// rows in the production_pointers table.
type ProductionPointerRepository struct{ q Queryer }

// NewProductionPointerRepository constructs a ProductionPointerRepository bound to the
// given Queryer. Pass *sqlx.DB for autocommit operations or *sqlx.Tx for
// transactional writes.
func NewProductionPointerRepository(q Queryer) *ProductionPointerRepository {
	return &ProductionPointerRepository{q: q}
}

var _ repository.ProductionPointerRepository = (*ProductionPointerRepository)(nil)

// productionPointerRow is the scan target for SELECT queries against the production_pointers table.
type productionPointerRow struct {
	ProjectName   string    `db:"project_name"`
	ReleaseID     string    `db:"release_id"`
	ManifestS3Key string    `db:"manifest_s3_key"`
	ImageTag      string    `db:"image_tag"`
	ManifestKind  string    `db:"manifest_kind"`
	UpdatedAt     time.Time `db:"updated_at"`
}

// List returns all per-project production pointers ordered by project name.
func (r *ProductionPointerRepository) List(ctx context.Context) ([]*release.ProductionPointer, error) {
	rows, err := r.q.QueryxContext(ctx,
		`SELECT project_name, release_id, manifest_s3_key, image_tag, manifest_kind, updated_at
		   FROM production_pointers
		  ORDER BY project_name`)
	if err != nil {
		return nil, fmt.Errorf("list production_pointers: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []*release.ProductionPointer
	for rows.Next() {
		var row productionPointerRow
		if err := rows.StructScan(&row); err != nil {
			return nil, fmt.Errorf("scan production_pointers row: %w", err)
		}
		out = append(out, release.NewProductionPointer(row.ProjectName, row.ReleaseID, row.ManifestS3Key, row.ImageTag, release.ManifestKind(row.ManifestKind), row.UpdatedAt))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate production_pointers rows: %w", err)
	}
	return out, nil
}

// Get returns the production pointer for the named project. When no row exists
// for that project, (nil, nil) is returned — not an error.
func (r *ProductionPointerRepository) Get(ctx context.Context, projectName string) (*release.ProductionPointer, error) {
	var row productionPointerRow
	err := r.q.GetContext(ctx, &row,
		`SELECT project_name, release_id, manifest_s3_key, image_tag, manifest_kind, updated_at
		   FROM production_pointers
		  WHERE project_name = $1`, projectName)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get production_pointers %q: %w", projectName, err)
	}
	return release.NewProductionPointer(row.ProjectName, row.ReleaseID, row.ManifestS3Key, row.ImageTag, release.ManifestKind(row.ManifestKind), row.UpdatedAt), nil
}

// Upsert inserts or updates the production pointer for the project recorded in
// pointer. If a row already exists for pointer.ProjectName(), it is fully replaced.
func (r *ProductionPointerRepository) Upsert(ctx context.Context, pointer *release.ProductionPointer) error {
	_, err := r.q.ExecContext(ctx,
		`INSERT INTO production_pointers (project_name, release_id, manifest_s3_key, image_tag, manifest_kind, updated_at)
		 VALUES ($1, $2, $3, $4, $5, $6)
		 ON CONFLICT (project_name) DO UPDATE SET
		   release_id      = EXCLUDED.release_id,
		   manifest_s3_key = EXCLUDED.manifest_s3_key,
		   image_tag       = EXCLUDED.image_tag,
		   manifest_kind   = EXCLUDED.manifest_kind,
		   updated_at      = EXCLUDED.updated_at`,
		pointer.ProjectName(), pointer.ReleaseID(), pointer.ManifestS3Key(), pointer.ImageTag(), string(pointer.ManifestKind()), pointer.UpdatedAt())
	if err != nil {
		return fmt.Errorf("upsert production_pointers %q: %w", pointer.ProjectName(), err)
	}
	return nil
}

// ProjectsWithKind returns the names of the projects whose pointer has the
// given manifest kind, sorted ascending. When none has it the list is empty.
func (r *ProductionPointerRepository) ProjectsWithKind(ctx context.Context, kind release.ManifestKind) ([]string, error) {
	rows, err := r.q.QueryxContext(ctx,
		`SELECT project_name
		   FROM production_pointers
		  WHERE manifest_kind = $1
		  ORDER BY project_name`, string(kind))
	if err != nil {
		return nil, fmt.Errorf("list production_pointers of kind %q: %w", kind, err)
	}
	defer func() { _ = rows.Close() }()

	names := []string{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("scan production_pointers name: %w", err)
		}
		names = append(names, name)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate production_pointers of kind %q: %w", kind, err)
	}
	return names, nil
}
