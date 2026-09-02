package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/carolsimone/continuo/release-controller/domain/pipeline"
	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/carolsimone/continuo/release-controller/domain/repository"
	"github.com/carolsimone/continuo/release-controller/service/ports"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
)

// Queryer is the minimal sqlx surface used by repository implementations,
// satisfied by both *sqlx.DB and *sqlx.Tx.
type Queryer interface {
	GetContext(ctx context.Context, dest any, query string, args ...any) error
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryRowxContext(ctx context.Context, query string, args ...any) *sqlx.Row
	QueryxContext(ctx context.Context, query string, args ...any) (*sqlx.Rows, error)
}

// RunRepository is the Postgres-backed repository.RunRepository over the
// release_pipeline_runs table.
type RunRepository struct {
	q       Queryer
	deleter ports.CandidateSQLDeleter
}

// NewRunRepository binds a RunRepository to q (a *sqlx.DB for autocommit
// reads, a *sqlx.Tx for transactional writes) and to the S3 deleter the prune
// uses; deleter may be nil in tests that never prune.
func NewRunRepository(q Queryer, deleter ports.CandidateSQLDeleter) *RunRepository {
	return &RunRepository{q: q, deleter: deleter}
}

var _ repository.RunRepository = (*RunRepository)(nil)

const runColumns = `run_id, run_kind, status, image_tags, changed_service,
	candidate_topology, validation_node_ids, fail_reason, fail_detail, failing_nodes,
	per_node_results, created_at, transitions, code_bundle_uri, manifest_kind,
	bootstrap, repo, commit_sha, remediation_round, rejection_payload,
	verifies_release_id, attempt, source_overlay_uri`

const activeStatuses = `('compiling','parsing','seed_building','validating')`

type runRow struct {
	RunID             string         `db:"run_id"`
	RunKind           string         `db:"run_kind"`
	Status            string         `db:"status"`
	ImageTagsJSON     []byte         `db:"image_tags"`
	ChangedService    string         `db:"changed_service"`
	CandidateTopology []byte         `db:"candidate_topology"`
	ValidationNodeIDs pq.StringArray `db:"validation_node_ids"`
	FailReason        sql.NullString `db:"fail_reason"`
	FailDetail        string         `db:"fail_detail"`
	FailingNodes      pq.StringArray `db:"failing_nodes"`
	PerNodeResults    []byte         `db:"per_node_results"`
	CreatedAt         sql.NullTime   `db:"created_at"`
	TransitionsJSON   []byte         `db:"transitions"`
	CodeBundleURI     string         `db:"code_bundle_uri"`
	ManifestKind      string         `db:"manifest_kind"`
	Bootstrap         bool           `db:"bootstrap"`
	Repo              string         `db:"repo"`
	CommitSHA         string         `db:"commit_sha"`
	RemediationRound  int            `db:"remediation_round"`
	RejectionPayload  []byte         `db:"rejection_payload"`
	VerifiesReleaseID string         `db:"verifies_release_id"`
	Attempt           int            `db:"attempt"`
	SourceOverlayURI  string         `db:"source_overlay_uri"`
}

func (r *RunRepository) getOne(ctx context.Context, where string, args ...any) (*pipeline.Run, error) {
	var row runRow
	err := r.q.GetContext(ctx, &row, `SELECT `+runColumns+` FROM release_pipeline_runs `+where, args...)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return rowToRun(row)
}

// Get returns the run with the given id, or nil if it does not exist.
func (r *RunRepository) Get(ctx context.Context, id string) (*pipeline.Run, error) {
	run, err := r.getOne(ctx, `WHERE run_id = $1`, id)
	if err != nil {
		return nil, fmt.Errorf("select run: %w", err)
	}
	return run, nil
}

// Load returns the run under a row-level FOR UPDATE lock, or nil if absent.
func (r *RunRepository) Load(ctx context.Context, id string) (*pipeline.Run, error) {
	run, err := r.getOne(ctx, `WHERE run_id = $1 FOR UPDATE`, id)
	if err != nil {
		return nil, fmt.Errorf("select run for update: %w", err)
	}
	return run, nil
}

// NextQueued returns the oldest received run of either kind, or nil.
func (r *RunRepository) NextQueued(ctx context.Context) (*pipeline.Run, error) {
	run, err := r.getOne(ctx, `WHERE status = 'received' ORDER BY created_at ASC, run_id ASC LIMIT 1`)
	if err != nil {
		return nil, fmt.Errorf("select next queued: %w", err)
	}
	return run, nil
}

// Active returns the single run of either kind that is currently in a leg, or nil.
func (r *RunRepository) Active(ctx context.Context) (*pipeline.Run, error) {
	run, err := r.getOne(ctx, `WHERE status IN `+activeStatuses+` ORDER BY created_at ASC, run_id ASC LIMIT 1`)
	if err != nil {
		return nil, fmt.Errorf("select active: %w", err)
	}
	return run, nil
}

// Save upserts the run keyed on run_id. Immutable facts (kind, service,
// created_at, manifest kind, provenance, verification facts) are written only
// on INSERT; the ON CONFLICT clause updates the mutable ones. image_tags is
// mutable because SetAssembledImageTags overwrites it at activation.
func (r *RunRepository) Save(ctx context.Context, run *pipeline.Run) error {
	imageTagsJSON, err := json.Marshal(run.ImageTags())
	if err != nil {
		return fmt.Errorf("marshal image_tags: %w", err)
	}
	topoJSON, err := json.Marshal(run.CandidateTopology())
	if err != nil {
		return fmt.Errorf("marshal topology: %w", err)
	}
	transitionsJSON, err := json.Marshal(run.Transitions())
	if err != nil {
		return fmt.Errorf("marshal transitions: %w", err)
	}
	perNodeJSON, err := json.Marshal(run.PerNodeResults())
	if err != nil {
		return fmt.Errorf("marshal per_node_results: %w", err)
	}
	failReason := sql.NullString{String: run.FailReason(), Valid: run.FailReason() != ""}
	// A nil payload must round-trip as SQL NULL, not an empty jsonb value.
	var rejectionPayload []byte
	if p := run.RejectionPayload(); len(p) > 0 {
		rejectionPayload = p
	}
	_, err = r.q.ExecContext(ctx,
		`INSERT INTO release_pipeline_runs (`+runColumns+`)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23)
		 ON CONFLICT (run_id) DO UPDATE SET
		   status = EXCLUDED.status,
		   image_tags = EXCLUDED.image_tags,
		   candidate_topology = EXCLUDED.candidate_topology,
		   validation_node_ids = EXCLUDED.validation_node_ids,
		   fail_reason = EXCLUDED.fail_reason,
		   fail_detail = EXCLUDED.fail_detail,
		   failing_nodes = EXCLUDED.failing_nodes,
		   per_node_results = EXCLUDED.per_node_results,
		   transitions = EXCLUDED.transitions,
		   code_bundle_uri = EXCLUDED.code_bundle_uri,
		   remediation_round = EXCLUDED.remediation_round,
		   rejection_payload = EXCLUDED.rejection_payload`,
		run.ID(), string(run.Kind()), string(run.Status()), imageTagsJSON, run.ChangedService(),
		topoJSON, pq.StringArray(run.ValidationNodeIDs()), failReason, run.FailDetail(), pq.StringArray(run.FailingNodes()),
		perNodeJSON, run.CreatedAt(), transitionsJSON, run.CodeBundleURI(), string(run.ManifestKind()),
		run.IsBootstrap(), run.Repo(), run.CommitSHA(), max(run.RemediationRound(), 1), rejectionPayload,
		run.VerifiesReleaseID(), run.Attempt(), run.SourceOverlayURI())
	if err != nil {
		return fmt.Errorf("upsert run: %w", err)
	}
	return nil
}

func rowToRun(row runRow) (*pipeline.Run, error) {
	imageTags := map[string]string{}
	if len(row.ImageTagsJSON) > 0 {
		if err := json.Unmarshal(row.ImageTagsJSON, &imageTags); err != nil {
			return nil, fmt.Errorf("unmarshal image_tags: %w", err)
		}
	}
	var topo release.Topology
	if len(row.CandidateTopology) > 0 {
		if err := json.Unmarshal(row.CandidateTopology, &topo); err != nil {
			return nil, fmt.Errorf("unmarshal candidate_topology: %w", err)
		}
	}
	var perNode []pipeline.NodeValidationResult
	if len(row.PerNodeResults) > 0 {
		if err := json.Unmarshal(row.PerNodeResults, &perNode); err != nil {
			return nil, fmt.Errorf("unmarshal per_node_results: %w", err)
		}
	}
	var transitions []pipeline.Transition
	if len(row.TransitionsJSON) > 0 {
		if err := json.Unmarshal(row.TransitionsJSON, &transitions); err != nil {
			return nil, fmt.Errorf("unmarshal transitions: %w", err)
		}
	}
	return pipeline.Rehydrate(pipeline.RehydrateInput{
		ID:                row.RunID,
		Kind:              pipeline.Kind(row.RunKind),
		Status:            pipeline.Status(row.Status),
		ImageTags:         imageTags,
		ChangedService:    row.ChangedService,
		ManifestKind:      release.ManifestKind(row.ManifestKind),
		CandidateTopology: topo,
		ValidationNodeIDs: []string(row.ValidationNodeIDs),
		PerNodeResults:    perNode,
		FailReason:        row.FailReason.String,
		FailDetail:        row.FailDetail,
		FailingNodes:      []string(row.FailingNodes),
		CodeBundleURI:     row.CodeBundleURI,
		CreatedAt:         row.CreatedAt.Time,
		Transitions:       transitions,
		Bootstrap:         row.Bootstrap,
		Repo:              row.Repo,
		CommitSHA:         row.CommitSHA,
		RemediationRound:  row.RemediationRound,
		RejectionPayload:  row.RejectionPayload,
		VerifiesReleaseID: row.VerifiesReleaseID,
		Attempt:           row.Attempt,
		SourceOverlayURI:  row.SourceOverlayURI,
	}), nil
}

// List returns a page of runs newest-first, narrowed by f. The cursor is
// keyset-based (created_at, run_id) so it is stable under concurrent inserts.
func (r *RunRepository) List(ctx context.Context, f repository.ListFilter) ([]*pipeline.Run, *repository.ListCursor, error) {
	limit := f.Limit
	if limit <= 0 || limit > 100 {
		limit = 20
	}
	var (
		args  []any
		conds []string
	)
	if f.Kind != nil {
		args = append(args, string(*f.Kind))
		conds = append(conds, fmt.Sprintf("run_kind = $%d", len(args)))
	}
	if f.Status != nil {
		args = append(args, *f.Status)
		conds = append(conds, fmt.Sprintf("status = $%d", len(args)))
	}
	if f.VerifiesReleaseID != nil {
		args = append(args, *f.VerifiesReleaseID)
		conds = append(conds, fmt.Sprintf("verifies_release_id = $%d", len(args)))
	}
	if f.Cursor != nil {
		args = append(args, f.Cursor.CreatedAt, f.Cursor.RunID)
		conds = append(conds, fmt.Sprintf("(created_at, run_id) < ($%d, $%d)", len(args)-1, len(args)))
	}
	where := ""
	if len(conds) > 0 {
		where = "WHERE " + strings.Join(conds, " AND ")
	}
	args = append(args, limit+1)
	query := fmt.Sprintf(`SELECT `+runColumns+` FROM release_pipeline_runs %s
	 ORDER BY created_at DESC, run_id DESC LIMIT $%d`, where, len(args))
	rows, err := r.q.QueryxContext(ctx, query, args...)
	if err != nil {
		return nil, nil, fmt.Errorf("list runs: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []*pipeline.Run
	for rows.Next() {
		var row runRow
		if err := rows.StructScan(&row); err != nil {
			return nil, nil, fmt.Errorf("scan run row: %w", err)
		}
		run, err := rowToRun(row)
		if err != nil {
			return nil, nil, err
		}
		out = append(out, run)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("iterate run rows: %w", err)
	}
	var next *repository.ListCursor
	if len(out) > limit {
		last := out[limit-1]
		next = &repository.ListCursor{CreatedAt: last.CreatedAt(), RunID: last.ID()}
		out = out[:limit]
	}
	return out, next, nil
}

// DeleteFinishedBefore removes terminal runs of either kind created before
// cutoff and not in keepIDs, then deletes each removed run's candidate-SQL
// and code-bundle prefixes from S3 (soft-fail; the bucket lifecycle rule is
// the backstop). Runs inside the caller's transaction; see the prune handler.
func (r *RunRepository) DeleteFinishedBefore(ctx context.Context, cutoff time.Time, keepIDs []string) (int, error) {
	rows, err := r.q.QueryxContext(ctx,
		`DELETE FROM release_pipeline_runs
		 WHERE status IN ('promoted','rejected','superseded','passed','failed')
		   AND created_at < $1
		   AND run_id <> ALL($2)
		 RETURNING run_id`,
		cutoff, pq.Array(keepIDs))
	if err != nil {
		return 0, fmt.Errorf("delete finished runs: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return 0, fmt.Errorf("scan deleted run_id: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("iterate deleted run ids: %w", err)
	}
	if r.deleter != nil {
		for _, id := range ids {
			_ = r.deleter.DeletePrefix(ctx, "candidate-sql/"+id+"/")
			_ = r.deleter.DeletePrefix(ctx, "code-bundles/"+id+"/")
		}
	}
	return len(ids), nil
}
