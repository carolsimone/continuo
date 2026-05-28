package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

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
	ChangedNodeIDs    pq.StringArray `db:"changed_node_ids"`
	ImageTagsJSON     []byte         `db:"image_tags"`
	ManifestsURI      string         `db:"manifests_uri"`
	CandidateTopology []byte         `db:"candidate_topology"`
	ValidationNodeIDs pq.StringArray `db:"validation_node_ids"`
	RejectReason      sql.NullString `db:"reject_reason"`
	FailingNodes      pq.StringArray `db:"failing_nodes"`
	DBTLogsURI        sql.NullString `db:"dbt_logs_uri"`
	CreatedAt         sql.NullTime   `db:"created_at"`
	TransitionsJSON   []byte         `db:"transitions"`
}

// Get returns the Release with the given ID, or nil if it does not exist.
func (r *ReleaseRepository) Get(ctx context.Context, id string) (*release.Release, error) {
	var row releaseRow
	err := r.q.GetContext(ctx, &row,
		`SELECT release_id, status, changed_node_ids, image_tags, manifests_uri,
		        candidate_topology, validation_node_ids, reject_reason, failing_nodes,
		        dbt_logs_uri, created_at, transitions
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
		`SELECT release_id, status, changed_node_ids, image_tags, manifests_uri,
		        candidate_topology, validation_node_ids, reject_reason, failing_nodes,
		        dbt_logs_uri, created_at, transitions
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
		`SELECT release_id, status, changed_node_ids, image_tags, manifests_uri,
		        candidate_topology, validation_node_ids, reject_reason, failing_nodes,
		        dbt_logs_uri, created_at, transitions
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
// fields (changed_node_ids, image_tags, manifests_uri, created_at) are only
// written on INSERT; the ON CONFLICT clause updates the mutable fields.
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
	dbtLogsURI := sql.NullString{String: rel.DBTLogsURI(), Valid: rel.DBTLogsURI() != ""}

	_, err = r.q.ExecContext(ctx,
		`INSERT INTO releases (
		   release_id, status, changed_node_ids, image_tags, manifests_uri,
		   candidate_topology, validation_node_ids, reject_reason, failing_nodes,
		   dbt_logs_uri, created_at, transitions
		 ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
		 ON CONFLICT (release_id) DO UPDATE SET
		   status = EXCLUDED.status,
		   candidate_topology = EXCLUDED.candidate_topology,
		   validation_node_ids = EXCLUDED.validation_node_ids,
		   reject_reason = EXCLUDED.reject_reason,
		   failing_nodes = EXCLUDED.failing_nodes,
		   dbt_logs_uri = EXCLUDED.dbt_logs_uri,
		   transitions = EXCLUDED.transitions`,
		rel.ID(), string(rel.Status()), pq.StringArray(rel.ChangedNodeIDs()),
		imageTagsJSON, rel.ManifestsURI(), topoJSON, pq.StringArray(rel.ValidationNodeIDs()),
		rejectReason, pq.StringArray(rel.FailingNodes()), dbtLogsURI,
		rel.CreatedAt(), transitionsJSON)
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
	return release.Rehydrate(release.RehydrateInput{
		ID:                row.ReleaseID,
		Status:            release.Status(row.Status),
		ChangedNodeIDs:    []string(row.ChangedNodeIDs),
		ImageTags:         imageTags,
		ManifestsURI:      row.ManifestsURI,
		CandidateTopology: topo,
		ValidationNodeIDs: []string(row.ValidationNodeIDs),
		RejectReason:      row.RejectReason.String,
		FailingNodes:      []string(row.FailingNodes),
		DBTLogsURI:        row.DBTLogsURI.String,
		CreatedAt:         row.CreatedAt.Time,
		Transitions:       transitions,
	}), nil
}
