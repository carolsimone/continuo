package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/carolsimone/continuo/release-controller/domain/repository"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
)

// Queryer is the minimal sqlx surface used by repository implementations. It
// is satisfied by both *sqlx.DB and *sqlx.Tx, allowing the same repo to work
// inside or outside a transaction.
type Queryer interface {
	GetContext(ctx context.Context, dest any, query string, args ...any) error
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryRowxContext(ctx context.Context, query string, args ...any) *sqlx.Row
	QueryxContext(ctx context.Context, query string, args ...any) (*sqlx.Rows, error)
}

// ReleaseRepository is the Postgres-backed implementation of
// repository.ReleaseRepository.
type ReleaseRepository struct{ q Queryer }

// NewReleaseRepository constructs a ReleaseRepository bound to the given
// Queryer. Pass *sqlx.DB for autocommit operations or *sqlx.Tx for
// transactional writes.
func NewReleaseRepository(q Queryer) *ReleaseRepository { return &ReleaseRepository{q: q} }

var _ repository.ReleaseRepository = (*ReleaseRepository)(nil)

// releaseRow is the scan target for SELECT queries against the releases table.
type releaseRow struct {
	ReleaseID         string         `db:"release_id"`
	Status            string         `db:"status"`
	ImageTagsJSON     []byte         `db:"image_tags"`
	ManifestsURI      string         `db:"manifests_uri"`
	CandidateTopology []byte         `db:"candidate_topology"`
	ValidationNodeIDs pq.StringArray `db:"validation_node_ids"`
	RejectReason      sql.NullString `db:"reject_reason"`
	FailingNodes      pq.StringArray `db:"failing_nodes"`
	PerNodeResults    []byte         `db:"per_node_results"`
	CreatedAt         sql.NullTime   `db:"created_at"`
	TransitionsJSON   []byte         `db:"transitions"`
	Bootstrap         bool           `db:"bootstrap"`
}

// Get returns the Release with the given ID, or nil if it does not exist.
func (r *ReleaseRepository) Get(ctx context.Context, id string) (*release.Release, error) {
	var row releaseRow
	err := r.q.GetContext(ctx, &row,
		`SELECT release_id, status, image_tags, manifests_uri,
		        candidate_topology, validation_node_ids, reject_reason, failing_nodes,
		        per_node_results, created_at, transitions, bootstrap
		 FROM releases WHERE release_id = $1`, id)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("select release: %w", err)
	}
	return rowToRelease(row)
}

// NextQueuedRelease returns the oldest Release in StatusReceived, or nil if
// there are none queued.
func (r *ReleaseRepository) NextQueuedRelease(ctx context.Context) (*release.Release, error) {
	var row releaseRow
	err := r.q.GetContext(ctx, &row,
		`SELECT release_id, status, image_tags, manifests_uri,
		        candidate_topology, validation_node_ids, reject_reason, failing_nodes,
		        per_node_results, created_at, transitions, bootstrap
		 FROM releases WHERE status = 'received'
		 ORDER BY created_at ASC, release_id ASC LIMIT 1`)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("select next queued: %w", err)
	}
	return rowToRelease(row)
}

// ActiveRelease returns the single Release that is currently parsing or
// validating, or nil if there is none.
func (r *ReleaseRepository) ActiveRelease(ctx context.Context) (*release.Release, error) {
	var row releaseRow
	err := r.q.GetContext(ctx, &row,
		`SELECT release_id, status, image_tags, manifests_uri,
		        candidate_topology, validation_node_ids, reject_reason, failing_nodes,
		        per_node_results, created_at, transitions, bootstrap
		 FROM releases WHERE status IN ('parsing','validating')
		 ORDER BY created_at ASC, release_id ASC LIMIT 1`)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("select active: %w", err)
	}
	return rowToRelease(row)
}

// Save persists a Release using an upsert keyed on release_id. Immutable
// fields (image_tags, manifests_uri, created_at) are only written on INSERT;
// the ON CONFLICT clause updates the mutable fields.
func (r *ReleaseRepository) Save(ctx context.Context, rel *release.Release) error {
	imageTagsJSON, err := json.Marshal(rel.ImageTags())
	if err != nil {
		return fmt.Errorf("marshal image_tags: %w", err)
	}
	topoJSON, err := json.Marshal(rel.CandidateTopology())
	if err != nil {
		return fmt.Errorf("marshal topology: %w", err)
	}
	transitionsJSON, err := json.Marshal(rel.Transitions())
	if err != nil {
		return fmt.Errorf("marshal transitions: %w", err)
	}
	rejectReason := sql.NullString{String: rel.RejectReason(), Valid: rel.RejectReason() != ""}
	perNodeJSON, err := json.Marshal(rel.PerNodeResults())
	if err != nil {
		return fmt.Errorf("marshal per_node_results: %w", err)
	}

	_, err = r.q.ExecContext(ctx,
		`INSERT INTO releases (
		   release_id, status, image_tags, manifests_uri,
		   candidate_topology, validation_node_ids, reject_reason, failing_nodes,
		   per_node_results, created_at, transitions, bootstrap
		 ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
		 ON CONFLICT (release_id) DO UPDATE SET
		   status = EXCLUDED.status,
		   candidate_topology = EXCLUDED.candidate_topology,
		   validation_node_ids = EXCLUDED.validation_node_ids,
		   reject_reason = EXCLUDED.reject_reason,
		   failing_nodes = EXCLUDED.failing_nodes,
		   per_node_results = EXCLUDED.per_node_results,
		   transitions = EXCLUDED.transitions`,
		rel.ID(), string(rel.Status()),
		imageTagsJSON, rel.ManifestsURI(), topoJSON, pq.StringArray(rel.ValidationNodeIDs()),
		rejectReason, pq.StringArray(rel.FailingNodes()), perNodeJSON,
		rel.CreatedAt(), transitionsJSON, rel.IsBootstrap())
	if err != nil {
		return fmt.Errorf("upsert release: %w", err)
	}
	return nil
}

// rowToRelease converts a releaseRow scan result into a domain Release.
func rowToRelease(row releaseRow) (*release.Release, error) {
	imageTags := map[string]string{}
	if len(row.ImageTagsJSON) > 0 {
		if err := json.Unmarshal(row.ImageTagsJSON, &imageTags); err != nil {
			return nil, fmt.Errorf("unmarshal image_tags: %w", err)
		}
	}
	var topo release.Topology
	if len(row.CandidateTopology) > 0 {
		if err := json.Unmarshal(row.CandidateTopology, &topo); err != nil {
			return nil, fmt.Errorf("unmarshal topology: %w", err)
		}
	}
	var transitions []release.Transition
	if len(row.TransitionsJSON) > 0 {
		if err := json.Unmarshal(row.TransitionsJSON, &transitions); err != nil {
			return nil, fmt.Errorf("unmarshal transitions: %w", err)
		}
	}
	var perNode []release.NodeValidationResult
	if len(row.PerNodeResults) > 0 {
		if err := json.Unmarshal(row.PerNodeResults, &perNode); err != nil {
			return nil, fmt.Errorf("unmarshal per_node_results: %w", err)
		}
	}
	return release.Rehydrate(release.RehydrateInput{
		ID:                row.ReleaseID,
		Status:            release.Status(row.Status),
		ImageTags:         imageTags,
		ManifestsURI:      row.ManifestsURI,
		CandidateTopology: topo,
		ValidationNodeIDs: []string(row.ValidationNodeIDs),
		RejectReason:      row.RejectReason.String,
		FailingNodes:      []string(row.FailingNodes),
		PerNodeResults:    perNode,
		CreatedAt:         row.CreatedAt.Time,
		Transitions:       transitions,
		Bootstrap:         row.Bootstrap,
	}), nil
}

// List returns a paginated list of releases ordered newest-first. The cursor
// is keyset-based (created_at, release_id) so it is stable under concurrent
// inserts.
func (r *ReleaseRepository) List(ctx context.Context, f repository.ListFilter) ([]*release.Release, *repository.ListCursor, error) {
	limit := f.Limit
	if limit <= 0 || limit > 100 {
		limit = 20
	}
	var (
		args  []any
		conds []string
	)
	if f.Status != nil {
		args = append(args, *f.Status)
		conds = append(conds, fmt.Sprintf("status = $%d", len(args)))
	}
	if f.Cursor != nil {
		args = append(args, f.Cursor.CreatedAt, f.Cursor.ReleaseID)
		conds = append(conds, fmt.Sprintf("(created_at, release_id) < ($%d, $%d)", len(args)-1, len(args)))
	}
	where := ""
	if len(conds) > 0 {
		where = "WHERE " + strings.Join(conds, " AND ")
	}
	args = append(args, limit+1)
	query := fmt.Sprintf(`SELECT release_id, status, image_tags, manifests_uri,
	        candidate_topology, validation_node_ids, reject_reason, failing_nodes,
	        per_node_results, created_at, transitions, bootstrap
	 FROM releases %s
	 ORDER BY created_at DESC, release_id DESC
	 LIMIT $%d`, where, len(args))

	rows, err := r.q.QueryxContext(ctx, query, args...)
	if err != nil {
		return nil, nil, fmt.Errorf("list releases: %w", err)
	}
	defer rows.Close()

	var out []*release.Release
	for rows.Next() {
		var row releaseRow
		if err := rows.StructScan(&row); err != nil {
			return nil, nil, fmt.Errorf("scan release row: %w", err)
		}
		rel, err := rowToRelease(row)
		if err != nil {
			return nil, nil, err
		}
		out = append(out, rel)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("iterate release rows: %w", err)
	}

	var next *repository.ListCursor
	if len(out) > limit {
		last := out[limit-1]
		next = &repository.ListCursor{CreatedAt: last.CreatedAt(), ReleaseID: last.ID()}
		out = out[:limit]
	}
	return out, next, nil
}

// DeleteResolvedBefore removes releases that are in a terminal state
// (promoted, rejected, or superseded), were created before the given cutoff,
// and are not the release identified by keepReleaseID. Returns the number of
// rows deleted.
func (r *ReleaseRepository) DeleteResolvedBefore(ctx context.Context, cutoff time.Time, keepReleaseID string) (int, error) {
	res, err := r.q.ExecContext(ctx,
		`DELETE FROM releases
		 WHERE status IN ('promoted','rejected','superseded')
		   AND created_at < $1
		   AND release_id <> $2`, cutoff, keepReleaseID)
	if err != nil {
		return 0, fmt.Errorf("delete resolved releases: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("rows affected: %w", err)
	}
	return int(n), nil
}
