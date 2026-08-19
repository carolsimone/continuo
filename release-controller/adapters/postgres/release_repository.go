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
	"github.com/carolsimone/continuo/release-controller/service/ports"
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
type ReleaseRepository struct {
	q       Queryer
	deleter ports.CandidateSQLDeleter
}

// NewReleaseRepository constructs a ReleaseRepository bound to the given
// Queryer and CandidateSQLDeleter. Pass *sqlx.DB for autocommit operations or
// *sqlx.Tx for transactional writes. deleter may be nil in tests that do not
// exercise pruning; production always passes a real S3 client.
func NewReleaseRepository(q Queryer, deleter ports.CandidateSQLDeleter) *ReleaseRepository {
	return &ReleaseRepository{q: q, deleter: deleter}
}

var _ repository.ReleaseRepository = (*ReleaseRepository)(nil)

// releaseRow is the scan target for SELECT queries against the releases table.
type releaseRow struct {
	ReleaseID         string         `db:"release_id"`
	Status            string         `db:"status"`
	ImageTagsJSON     []byte         `db:"image_tags"`
	ChangedService    string         `db:"changed_service"`
	CandidateTopology []byte         `db:"candidate_topology"`
	ValidationNodeIDs pq.StringArray `db:"validation_node_ids"`
	RejectReason      sql.NullString `db:"reject_reason"`
	RejectDetail      string         `db:"reject_detail"`
	FailingNodes      pq.StringArray `db:"failing_nodes"`
	PerNodeResults    []byte         `db:"per_node_results"`
	CreatedAt         sql.NullTime   `db:"created_at"`
	TransitionsJSON   []byte         `db:"transitions"`
	Bootstrap         bool           `db:"bootstrap"`
	Shadow            bool           `db:"shadow"`
	Repo              string         `db:"repo"`
	CommitSHA         string         `db:"commit_sha"`
	CodeBundleURI     string         `db:"code_bundle_uri"`
	Kind              string         `db:"kind"`
}

// Get returns the Release with the given ID, or nil if it does not exist.
func (r *ReleaseRepository) Get(ctx context.Context, id string) (*release.Release, error) {
	var row releaseRow
	err := r.q.GetContext(ctx, &row,
		`SELECT release_id, status, image_tags, changed_service,
		        candidate_topology, validation_node_ids, reject_reason, reject_detail, failing_nodes,
		        per_node_results, created_at, transitions, bootstrap, shadow, repo, commit_sha, code_bundle_uri, kind
		 FROM releases WHERE release_id = $1`, id)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("select release: %w", err)
	}
	return rowToRelease(row)
}

// Load returns the Release with the given ID under a row-level FOR UPDATE lock,
// or nil if it does not exist. Callers must be inside a transaction; the lock
// serializes the terminal handler against concurrent per-node projection upserts.
func (r *ReleaseRepository) Load(ctx context.Context, id string) (*release.Release, error) {
	var row releaseRow
	err := r.q.GetContext(ctx, &row,
		`SELECT release_id, status, image_tags, changed_service,
		        candidate_topology, validation_node_ids, reject_reason, reject_detail, failing_nodes,
		        per_node_results, created_at, transitions, bootstrap, shadow, repo, commit_sha, code_bundle_uri, kind
		 FROM releases WHERE release_id = $1 FOR UPDATE`, id)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("select release for update: %w", err)
	}
	return rowToRelease(row)
}

// NextQueuedRelease returns the oldest Release in StatusReceived, or nil if
// there are none queued.
func (r *ReleaseRepository) NextQueuedRelease(ctx context.Context) (*release.Release, error) {
	var row releaseRow
	err := r.q.GetContext(ctx, &row,
		`SELECT release_id, status, image_tags, changed_service,
		        candidate_topology, validation_node_ids, reject_reason, reject_detail, failing_nodes,
		        per_node_results, created_at, transitions, bootstrap, shadow, repo, commit_sha, code_bundle_uri, kind
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

// ActiveRelease returns the single Release that is currently compiling,
// parsing, seed_building, or validating, or nil if there is none.
func (r *ReleaseRepository) ActiveRelease(ctx context.Context) (*release.Release, error) {
	var row releaseRow
	err := r.q.GetContext(ctx, &row,
		`SELECT release_id, status, image_tags, changed_service,
		        candidate_topology, validation_node_ids, reject_reason, reject_detail, failing_nodes,
		        per_node_results, created_at, transitions, bootstrap, shadow, repo, commit_sha, code_bundle_uri, kind
		 FROM releases WHERE status IN ('compiling','parsing','seed_building','validating')
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
// fields (image_tags, changed_service, created_at, repo, commit_sha, kind) are only written on INSERT;
// the ON CONFLICT clause updates the mutable fields. image_tags is also updated
// on conflict because SetAssembledImageTags overwrites it at advance-time.
// code_bundle_uri is likewise updated on conflict: it is unknown at receive
// time and only set once the parse result arrives (SetCodeBundleURI).
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
		   release_id, status, image_tags, changed_service,
		   candidate_topology, validation_node_ids, reject_reason, reject_detail, failing_nodes,
		   per_node_results, created_at, transitions, bootstrap, shadow, repo, commit_sha, code_bundle_uri, kind
		 ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)
		 ON CONFLICT (release_id) DO UPDATE SET
		   status = EXCLUDED.status,
		   image_tags = EXCLUDED.image_tags,
		   candidate_topology = EXCLUDED.candidate_topology,
		   validation_node_ids = EXCLUDED.validation_node_ids,
		   reject_reason = EXCLUDED.reject_reason,
		   reject_detail = EXCLUDED.reject_detail,
		   failing_nodes = EXCLUDED.failing_nodes,
		   per_node_results = EXCLUDED.per_node_results,
		   transitions = EXCLUDED.transitions,
		   code_bundle_uri = EXCLUDED.code_bundle_uri`,
		rel.ID(), string(rel.Status()),
		imageTagsJSON, rel.ChangedService(), topoJSON, pq.StringArray(rel.ValidationNodeIDs()),
		rejectReason, rel.RejectDetail(), pq.StringArray(rel.FailingNodes()), perNodeJSON,
		rel.CreatedAt(), transitionsJSON, rel.IsBootstrap(), rel.IsShadow(), rel.Repo(), rel.CommitSHA(), rel.CodeBundleURI(),
		string(rel.ManifestKind()))
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
		ChangedService:    row.ChangedService,
		CandidateTopology: topo,
		ValidationNodeIDs: []string(row.ValidationNodeIDs),
		RejectReason:      row.RejectReason.String,
		RejectDetail:      row.RejectDetail,
		FailingNodes:      []string(row.FailingNodes),
		PerNodeResults:    perNode,
		CreatedAt:         row.CreatedAt.Time,
		Transitions:       transitions,
		Bootstrap:         row.Bootstrap,
		Shadow:            row.Shadow,
		Repo:              row.Repo,
		CommitSHA:         row.CommitSHA,
		CodeBundleURI:     row.CodeBundleURI,
		ManifestKind:      release.ManifestKind(row.Kind),
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
	query := fmt.Sprintf(`SELECT release_id, status, image_tags, changed_service,
	        candidate_topology, validation_node_ids, reject_reason, reject_detail, failing_nodes,
	        per_node_results, created_at, transitions, bootstrap, shadow, repo, commit_sha, code_bundle_uri, kind
	 FROM releases %s
	 ORDER BY created_at DESC, release_id DESC
	 LIMIT $%d`, where, len(args))

	rows, err := r.q.QueryxContext(ctx, query, args...)
	if err != nil {
		return nil, nil, fmt.Errorf("list releases: %w", err)
	}
	defer func() { _ = rows.Close() }()

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
// (promoted, rejected, superseded, or validated), were created before the given cutoff,
// and are not in the keepReleaseIDs set. An empty keepReleaseIDs slice means
// no extra releases are preserved. Returns the number of rows deleted.
// After deleting each row, it also attempts to remove the corresponding
// candidate-SQL objects from S3 under candidate-sql/<release_id>/ and the
// release's code-bundle document under code-bundles/<release_id>/.
// S3 deletion is soft-fail: if it errors, the error is logged and the prune
// continues. A bucket lifecycle rule is the backstop for any objects left behind.
//
// The S3 deletes run inside the prune transaction, before the caller commits.
// This ordering is intentional: the failure it exposes (a post-delete commit
// failure leaving objects deleted for rows that survive) only ever orphans S3
// objects — never SQL still referenced by a live release — and the lifecycle rule
// reclaims those anyway.
func (r *ReleaseRepository) DeleteResolvedBefore(ctx context.Context, cutoff time.Time, keepReleaseIDs []string) (int, error) {
	rows, err := r.q.QueryxContext(ctx,
		`DELETE FROM releases
		 WHERE status IN ('promoted','rejected','superseded','validated')
		   AND created_at < $1
		   AND release_id <> ALL($2)
		 RETURNING release_id`,
		cutoff, pq.Array(keepReleaseIDs))
	if err != nil {
		return 0, fmt.Errorf("delete resolved releases: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return 0, fmt.Errorf("scan deleted release_id: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("iterate deleted release ids: %w", err)
	}

	if r.deleter != nil {
		for _, id := range ids {
			// Soft-fail: the S3 deleter logs its own failures; a cleanup error must
			// not abort the prune (the bucket lifecycle rule reclaims anything left).
			_ = r.deleter.DeletePrefix(ctx, "candidate-sql/"+id+"/")
			_ = r.deleter.DeletePrefix(ctx, "code-bundles/"+id+"/")
		}
	}

	return len(ids), nil
}
