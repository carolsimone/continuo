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

// serviceProdRow is the scan target for SELECT queries against the service_prod table.
type serviceProdRow struct {
	ServiceName   string    `db:"service_name"`
	ReleaseID     string    `db:"release_id"`
	ManifestS3Key string    `db:"manifest_s3_key"`
	ImageTag      string    `db:"image_tag"`
	UpdatedAt     time.Time `db:"updated_at"`
}

// List returns all per-service production pointers ordered by service name.
func (r *ServiceProdRepository) List(ctx context.Context) ([]*release.ServiceProd, error) {
	rows, err := r.q.QueryxContext(ctx,
		`SELECT service_name, release_id, manifest_s3_key, image_tag, updated_at
		   FROM service_prod
		  ORDER BY service_name`)
	if err != nil {
		return nil, fmt.Errorf("list service_prod: %w", err)
	}
	defer rows.Close()

	var out []*release.ServiceProd
	for rows.Next() {
		var row serviceProdRow
		if err := rows.StructScan(&row); err != nil {
			return nil, fmt.Errorf("scan service_prod row: %w", err)
		}
		out = append(out, release.NewServiceProd(row.ServiceName, row.ReleaseID, row.ManifestS3Key, row.ImageTag, row.UpdatedAt))
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
		`SELECT service_name, release_id, manifest_s3_key, image_tag, updated_at
		   FROM service_prod
		  WHERE service_name = $1`, serviceName)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get service_prod %q: %w", serviceName, err)
	}
	return release.NewServiceProd(row.ServiceName, row.ReleaseID, row.ManifestS3Key, row.ImageTag, row.UpdatedAt), nil
}

// Upsert inserts or updates the production pointer for the service recorded in
// sp. If a row already exists for sp.ServiceName(), it is fully replaced.
func (r *ServiceProdRepository) Upsert(ctx context.Context, sp *release.ServiceProd) error {
	_, err := r.q.ExecContext(ctx,
		`INSERT INTO service_prod (service_name, release_id, manifest_s3_key, image_tag, updated_at)
		 VALUES ($1, $2, $3, $4, $5)
		 ON CONFLICT (service_name) DO UPDATE SET
		   release_id      = EXCLUDED.release_id,
		   manifest_s3_key = EXCLUDED.manifest_s3_key,
		   image_tag       = EXCLUDED.image_tag,
		   updated_at      = EXCLUDED.updated_at`,
		sp.ServiceName(), sp.ReleaseID(), sp.ManifestS3Key(), sp.ImageTag(), sp.UpdatedAt())
	if err != nil {
		return fmt.Errorf("upsert service_prod %q: %w", sp.ServiceName(), err)
	}
	return nil
}
