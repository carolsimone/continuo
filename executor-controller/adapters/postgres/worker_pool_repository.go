package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/carolsimone/continuo/executor-controller/domain/model"
	"github.com/carolsimone/continuo/executor-controller/domain/repository"
	pkgmodel "github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/jmoiron/sqlx"
)

// workerPoolRow is one executor_worker_pools row. initialization_error is
// nullable in the table but a plain string on the aggregate, where empty means
// ready.
type workerPoolRow struct {
	PoolKey                           string         `db:"pool_key"`
	ServiceName                       string         `db:"service_name"`
	ImageTag                          string         `db:"image_tag"`
	RuntimeManifestURI                string         `db:"runtime_manifest_uri"`
	RuntimeManifestSHA256             string         `db:"runtime_manifest_sha256"`
	RuntimeManifestDBTVersion         string         `db:"runtime_manifest_dbt_version"`
	RuntimeManifestParseContextSHA256 string         `db:"runtime_manifest_parse_context_sha256"`
	CredentialSHA256                  string         `db:"credential_sha256"`
	DesiredReplicas                   int            `db:"desired_replicas"`
	LastActivityAt                    time.Time      `db:"last_activity_at"`
	InitializationError               sql.NullString `db:"initialization_error"`
	CreatedAt                         time.Time      `db:"created_at"`
	UpdatedAt                         time.Time      `db:"updated_at"`
}

// toModel rebuilds the aggregate from the row.
func (r workerPoolRow) toModel() model.WorkerPool {
	return model.WorkerPool{
		PoolKey:     r.PoolKey,
		ServiceName: r.ServiceName,
		ImageTag:    r.ImageTag,
		RuntimeManifest: pkgmodel.RuntimeManifestRef{
			RuntimeManifestURI:                r.RuntimeManifestURI,
			RuntimeManifestSHA256:             r.RuntimeManifestSHA256,
			RuntimeManifestDBTVersion:         r.RuntimeManifestDBTVersion,
			RuntimeManifestParseContextSHA256: r.RuntimeManifestParseContextSHA256,
		},
		CredentialSHA256:    r.CredentialSHA256,
		DesiredReplicas:     r.DesiredReplicas,
		LastActivityAt:      r.LastActivityAt,
		InitializationError: r.InitializationError.String,
		CreatedAt:           r.CreatedAt,
		UpdatedAt:           r.UpdatedAt,
	}
}

// workerPoolExecutor is the read/write subset of sqlx.DB / sqlx.Tx this
// repository needs.
type workerPoolExecutor interface {
	sqlx.QueryerContext
	sqlx.ExecerContext
}

type workerPoolRepository struct {
	executor workerPoolExecutor
}

var _ repository.WorkerPoolRepository = (*workerPoolRepository)(nil)

// NewWorkerPoolRepository creates a repository.WorkerPoolRepository against any
// sqlx executor (*sqlx.DB for autocommit, *sqlx.Tx for transaction-scoped work).
func NewWorkerPoolRepository(executor workerPoolExecutor) repository.WorkerPoolRepository {
	return &workerPoolRepository{executor: executor}
}

const workerPoolColumns = `
	pool_key, service_name, image_tag,
	runtime_manifest_uri, runtime_manifest_sha256,
	runtime_manifest_dbt_version, runtime_manifest_parse_context_sha256,
	credential_sha256, desired_replicas, last_activity_at,
	initialization_error, created_at, updated_at`

func (r *workerPoolRepository) Get(ctx context.Context, poolKey string) (*model.WorkerPool, error) {
	var row workerPoolRow
	err := sqlx.GetContext(ctx, r.executor, &row,
		`SELECT `+workerPoolColumns+` FROM executor_worker_pools WHERE pool_key = $1`, poolKey)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get worker pool %s: %w", poolKey, err)
	}
	pool := row.toModel()
	return &pool, nil
}

func (r *workerPoolRepository) Add(ctx context.Context, pool model.WorkerPool) error {
	_, err := r.executor.ExecContext(ctx, `
		INSERT INTO executor_worker_pools (`+workerPoolColumns+`)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)`,
		pool.PoolKey, pool.ServiceName, pool.ImageTag,
		pool.RuntimeManifest.RuntimeManifestURI, pool.RuntimeManifest.RuntimeManifestSHA256,
		pool.RuntimeManifest.RuntimeManifestDBTVersion,
		pool.RuntimeManifest.RuntimeManifestParseContextSHA256,
		pool.CredentialSHA256, pool.DesiredReplicas, pool.LastActivityAt,
		nullString(pool.InitializationError), pool.CreatedAt, pool.UpdatedAt)
	if err != nil {
		return fmt.Errorf("add worker pool %s: %w", pool.PoolKey, err)
	}
	return nil
}

// Save writes back a registered pool. pool_key and created_at identify the row
// and are not rewritten. A pool that was never registered is reported rather
// than silently ignored: a report about a pool the executor does not know is a
// fault, not a no-op.
func (r *workerPoolRepository) Save(ctx context.Context, pool model.WorkerPool) error {
	res, err := r.executor.ExecContext(ctx, `
		UPDATE executor_worker_pools SET
			service_name = $2, image_tag = $3,
			runtime_manifest_uri = $4, runtime_manifest_sha256 = $5,
			runtime_manifest_dbt_version = $6, runtime_manifest_parse_context_sha256 = $7,
			credential_sha256 = $8, desired_replicas = $9, last_activity_at = $10,
			initialization_error = $11, updated_at = $12
		WHERE pool_key = $1`,
		pool.PoolKey, pool.ServiceName, pool.ImageTag,
		pool.RuntimeManifest.RuntimeManifestURI, pool.RuntimeManifest.RuntimeManifestSHA256,
		pool.RuntimeManifest.RuntimeManifestDBTVersion,
		pool.RuntimeManifest.RuntimeManifestParseContextSHA256,
		pool.CredentialSHA256, pool.DesiredReplicas, pool.LastActivityAt,
		nullString(pool.InitializationError), pool.UpdatedAt)
	if err != nil {
		return fmt.Errorf("save worker pool %s: %w", pool.PoolKey, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("save worker pool %s: %w", pool.PoolKey, err)
	}
	if n == 0 {
		return fmt.Errorf("worker pool %s is not registered", pool.PoolKey)
	}
	return nil
}

// SaveInitializationError writes back only the columns a worker's
// initialization report owns. The pool's credential, replica count, and runtime
// artifact are left as the row holds them, so a report cannot carry a value
// read before a concurrent write back over it.
func (r *workerPoolRepository) SaveInitializationError(
	ctx context.Context, poolKey, initializationError string, at time.Time,
) error {
	res, err := r.executor.ExecContext(ctx, `
		UPDATE executor_worker_pools SET initialization_error = $2, updated_at = $3
		WHERE pool_key = $1`, poolKey, nullString(initializationError), at)
	if err != nil {
		return fmt.Errorf("save worker pool %s initialization error: %w", poolKey, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("save worker pool %s initialization error: %w", poolKey, err)
	}
	if n == 0 {
		return fmt.Errorf("worker pool %s is not registered", poolKey)
	}
	return nil
}

func (r *workerPoolRepository) List(ctx context.Context) ([]model.WorkerPool, error) {
	var rows []workerPoolRow
	if err := sqlx.SelectContext(ctx, r.executor, &rows,
		`SELECT `+workerPoolColumns+` FROM executor_worker_pools ORDER BY pool_key ASC`); err != nil {
		return nil, fmt.Errorf("list worker pools: %w", err)
	}
	pools := make([]model.WorkerPool, 0, len(rows))
	for _, row := range rows {
		pools = append(pools, row.toModel())
	}
	return pools, nil
}

// poolIdentityRow is the adapter-internal scan struct for ListUnregistered. The
// fields come out of a task's job_params, where a runtime manifest reference is
// stored with omitempty — so a task carrying an incomplete one yields NULLs
// rather than a missing key, and the query filters those rows out before here.
type poolIdentityRow struct {
	PoolKey      string `db:"pool_key"`
	ServiceName  string `db:"service_name"`
	ImageTag     string `db:"image_tag"`
	ManifestURI  string `db:"runtime_manifest_uri"`
	ManifestSHA  string `db:"runtime_manifest_sha256"`
	DBTVersion   string `db:"runtime_manifest_dbt_version"`
	ParseContext string `db:"runtime_manifest_parse_context_sha256"`
}

// ListUnregistered discovers the pools that must exist from the worker-mode work
// already waiting on them.
//
// Only work that can still run counts: 'pending' is a task waiting for a worker
// and 'retry_pending' one waiting out its backoff before it becomes one. Leased
// and running work is deliberately absent — a task in either state has a worker
// already, which means its pool is registered. Settled work is absent for the
// same reason it needs no worker: it is done.
//
// The identity is read from the newest task of each pool. A pool key digests the
// service, the image tag, and the runtime manifest, so every task sharing a key
// agrees on all three by construction and any of them would answer identically;
// taking one row per key is what makes a pool's whole backlog discover one pool.
//
// A task whose runtime manifest reference is incomplete is skipped. Such a
// record is rejected before it is ever enqueued, so this only ever excludes a
// row that predates the check — and a pool with no artifact to hydrate is one
// its workers could not initialize anyway.
func (r *workerPoolRepository) ListUnregistered(ctx context.Context) ([]model.PoolIdentity, error) {
	const query = `
		SELECT DISTINCT ON (d.pool_key)
		       d.pool_key,
		       d.job_params->>'service_name' AS service_name,
		       d.job_params->>'image_tag' AS image_tag,
		       d.job_params->>'runtime_manifest_uri' AS runtime_manifest_uri,
		       d.job_params->>'runtime_manifest_sha256' AS runtime_manifest_sha256,
		       d.job_params->>'runtime_manifest_dbt_version' AS runtime_manifest_dbt_version,
		       d.job_params->>'runtime_manifest_parse_context_sha256'
		           AS runtime_manifest_parse_context_sha256
		FROM executor_deployments d
		LEFT JOIN executor_worker_pools p ON p.pool_key = d.pool_key
		WHERE d.execution_mode = 'workers'
		  AND d.status IN ('pending', 'retry_pending')
		  AND p.pool_key IS NULL
		  AND d.job_params->>'runtime_manifest_uri' IS NOT NULL
		  AND d.job_params->>'runtime_manifest_sha256' IS NOT NULL
		  AND d.job_params->>'runtime_manifest_dbt_version' IS NOT NULL
		  AND d.job_params->>'runtime_manifest_parse_context_sha256' IS NOT NULL
		ORDER BY d.pool_key ASC, d.created_at DESC`

	var rows []poolIdentityRow
	if err := sqlx.SelectContext(ctx, r.executor, &rows, query); err != nil {
		return nil, fmt.Errorf("list unregistered worker pools: %w", err)
	}

	out := make([]model.PoolIdentity, 0, len(rows))
	for _, row := range rows {
		out = append(out, model.PoolIdentity{
			PoolKey:     row.PoolKey,
			ServiceName: row.ServiceName,
			ImageTag:    row.ImageTag,
			RuntimeManifest: pkgmodel.RuntimeManifestRef{
				RuntimeManifestURI:                row.ManifestURI,
				RuntimeManifestSHA256:             row.ManifestSHA,
				RuntimeManifestDBTVersion:         row.DBTVersion,
				RuntimeManifestParseContextSHA256: row.ParseContext,
			},
		})
	}
	return out, nil
}

// nullString maps the aggregate's empty-means-absent string onto a nullable
// column, so a cleared initialization error reads back as NULL rather than ''.
func nullString(s string) sql.NullString {
	return sql.NullString{String: s, Valid: s != ""}
}
