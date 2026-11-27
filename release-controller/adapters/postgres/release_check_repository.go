package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/carolsimone/continuo/pkg/contractbundle"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/topologyartifact"
	"github.com/carolsimone/continuo/release-controller/adapters/serialization"
	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/carolsimone/continuo/release-controller/domain/releasecheck"
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

// ReleaseCheckRepository is the Postgres-backed repository.ReleaseCheckRepository over the
// release_checks table.
type ReleaseCheckRepository struct {
	q       Queryer
	deleter ports.CandidateSQLDeleter
}

// NewReleaseCheckRepository binds a ReleaseCheckRepository to q (a *sqlx.DB for autocommit
// reads, a *sqlx.Tx for transactional writes) and to the S3 deleter the prune
// uses; deleter may be nil in tests that never prune.
func NewReleaseCheckRepository(q Queryer, deleter ports.CandidateSQLDeleter) *ReleaseCheckRepository {
	return &ReleaseCheckRepository{q: q, deleter: deleter}
}

var _ repository.ReleaseCheckRepository = (*ReleaseCheckRepository)(nil)

const checkColumns = `check_id, check_kind, status, image_tags, changed_project,
	validation_node_ids, fail_reason, fail_detail, failing_nodes,
	per_node_results, created_at, transitions, code_bundle_uri, manifest_kind,
	bootstrap, repo, commit_sha, remediation_round, rejection_payload,
	verifies_release_id, try, source_overlay_uri,
	candidate_topology_uri, candidate_topology_sha256, candidate_node_count,
	rebaseline, notices,
	candidate_contract_bundle_uri, candidate_contract_bundle_sha256, continuo_contracts,
	parse_ms, contracts_ms, contracts_by_family`

const activeStatuses = `('compiling','parsing','seed_building','validating')`

// terminalStatuses are the statuses that end a check of either kind.
const terminalStatuses = `('promoted','rejected','superseded','passed','failed')`

type checkRow struct {
	CheckID           string         `db:"check_id"`
	CheckKind         string         `db:"check_kind"`
	Status            string         `db:"status"`
	ImageTagsJSON     []byte         `db:"image_tags"`
	ChangedProject    string         `db:"changed_project"`
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
	Try               int            `db:"try"`
	SourceOverlayURI  string         `db:"source_overlay_uri"`

	CandidateTopologyURI    sql.NullString `db:"candidate_topology_uri"`
	CandidateTopologySHA256 sql.NullString `db:"candidate_topology_sha256"`
	CandidateNodeCount      sql.NullInt32  `db:"candidate_node_count"`
	Rebaseline              bool           `db:"rebaseline"`
	Notices                 []byte         `db:"notices"`

	CandidateContractBundleURI    string `db:"candidate_contract_bundle_uri"`
	CandidateContractBundleSHA256 string `db:"candidate_contract_bundle_sha256"`
	ContinuoContracts             string `db:"continuo_contracts"`

	ParseMs           sql.NullInt64 `db:"parse_ms"`
	ContractsMs       sql.NullInt64 `db:"contracts_ms"`
	ContractsByFamily []byte        `db:"contracts_by_family"`
}

func (r *ReleaseCheckRepository) getOne(ctx context.Context, where string, args ...any) (*releasecheck.Check, error) {
	var row checkRow
	err := r.q.GetContext(ctx, &row, `SELECT `+checkColumns+` FROM release_checks `+where, args...)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return rowToCheck(row)
}

// Get returns the check with the given id, or nil if it does not exist.
func (r *ReleaseCheckRepository) Get(ctx context.Context, id string) (*releasecheck.Check, error) {
	check, err := r.getOne(ctx, `WHERE check_id = $1`, id)
	if err != nil {
		return nil, fmt.Errorf("select check: %w", err)
	}
	return check, nil
}

// Load returns the check under a row-level FOR UPDATE lock, or nil if absent.
func (r *ReleaseCheckRepository) Load(ctx context.Context, id string) (*releasecheck.Check, error) {
	check, err := r.getOne(ctx, `WHERE check_id = $1 FOR UPDATE`, id)
	if err != nil {
		return nil, fmt.Errorf("select check for update: %w", err)
	}
	return check, nil
}

// NextQueued returns the oldest received check of either kind, or nil.
func (r *ReleaseCheckRepository) NextQueued(ctx context.Context) (*releasecheck.Check, error) {
	check, err := r.getOne(ctx, `WHERE status = 'received' ORDER BY created_at ASC, check_id ASC LIMIT 1`)
	if err != nil {
		return nil, fmt.Errorf("select next queued: %w", err)
	}
	return check, nil
}

// NextQueuedOfKind returns the oldest received check of the given kind, or nil.
func (r *ReleaseCheckRepository) NextQueuedOfKind(ctx context.Context, kind releasecheck.Kind) (*releasecheck.Check, error) {
	check, err := r.getOne(ctx, `WHERE status = 'received' AND check_kind = $1 ORDER BY created_at ASC, check_id ASC LIMIT 1`, string(kind))
	if err != nil {
		return nil, fmt.Errorf("select next queued %s: %w", kind, err)
	}
	return check, nil
}

// Active returns the single check of either kind that is currently in a leg, or nil.
func (r *ReleaseCheckRepository) Active(ctx context.Context) (*releasecheck.Check, error) {
	check, err := r.getOne(ctx, `WHERE status IN `+activeStatuses+` ORDER BY created_at ASC, check_id ASC LIMIT 1`)
	if err != nil {
		return nil, fmt.Errorf("select active: %w", err)
	}
	return check, nil
}

// ActiveCandidateProjectsWithKind lists the changed projects of the candidate
// checks of the given manifest kind that have not ended, sorted ascending and
// without repeats.
func (r *ReleaseCheckRepository) ActiveCandidateProjectsWithKind(ctx context.Context, kind release.ManifestKind) ([]string, error) {
	rows, err := r.q.QueryxContext(ctx,
		`SELECT DISTINCT changed_project
		   FROM release_checks
		  WHERE check_kind = 'candidate'
		    AND manifest_kind = $1
		    AND status NOT IN `+terminalStatuses+`
		  ORDER BY changed_project`, string(kind))
	if err != nil {
		return nil, fmt.Errorf("select active candidate projects of kind %q: %w", kind, err)
	}
	defer func() { _ = rows.Close() }()

	projects := []string{}
	for rows.Next() {
		var project string
		if err := rows.Scan(&project); err != nil {
			return nil, fmt.Errorf("scan active candidate project: %w", err)
		}
		projects = append(projects, project)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate active candidate projects of kind %q: %w", kind, err)
	}
	return projects, nil
}

// Save upserts the check keyed on check_id. Immutable facts (kind, project,
// created_at, manifest kind, provenance, fix facts) are written only
// on INSERT; the ON CONFLICT clause updates the mutable ones. image_tags is
// mutable because SetAssembledImageTags overwrites it at activation.
// A check stores the reference to its topology artifact, never the topology.
func (r *ReleaseCheckRepository) Save(ctx context.Context, check *releasecheck.Check) error {
	args, err := encodeCheck(check)
	if err != nil {
		return err
	}
	// The WHERE on the upsert refuses a cross-kind collision atomically: an id
	// that already names a check of the other kind matches on check_id but fails the
	// kind predicate, so no row is written and RowsAffected is 0. A save can
	// therefore never change a persisted check's kind, whatever the caller read
	// beforehand.
	res, err := r.q.ExecContext(ctx,
		`INSERT INTO release_checks (`+checkColumns+`)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32,$33)
		 ON CONFLICT (check_id) DO UPDATE SET
		   status = EXCLUDED.status,
		   image_tags = EXCLUDED.image_tags,
		   validation_node_ids = EXCLUDED.validation_node_ids,
		   fail_reason = EXCLUDED.fail_reason,
		   fail_detail = EXCLUDED.fail_detail,
		   failing_nodes = EXCLUDED.failing_nodes,
		   per_node_results = EXCLUDED.per_node_results,
		   transitions = EXCLUDED.transitions,
		   code_bundle_uri = EXCLUDED.code_bundle_uri,
		   remediation_round = EXCLUDED.remediation_round,
		   rejection_payload = EXCLUDED.rejection_payload,
		   candidate_topology_uri = EXCLUDED.candidate_topology_uri,
		   candidate_topology_sha256 = EXCLUDED.candidate_topology_sha256,
		   candidate_node_count = EXCLUDED.candidate_node_count,
		   notices = EXCLUDED.notices,
		   candidate_contract_bundle_uri = EXCLUDED.candidate_contract_bundle_uri,
		   candidate_contract_bundle_sha256 = EXCLUDED.candidate_contract_bundle_sha256,
		   continuo_contracts = EXCLUDED.continuo_contracts,
		   parse_ms = EXCLUDED.parse_ms,
		   contracts_ms = EXCLUDED.contracts_ms,
		   contracts_by_family = EXCLUDED.contracts_by_family
		 WHERE release_checks.check_kind = EXCLUDED.check_kind`,
		args...)
	if err != nil {
		return fmt.Errorf("upsert check: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return fmt.Errorf("save check %s: %w", check.ID(), repository.ErrCheckKindConflict)
	}
	return nil
}

// Create inserts the check only when check_id is free. ON CONFLICT DO NOTHING
// leaves an existing row untouched; when another transaction holds an
// uncommitted insert of the same id, this statement waits for it, so a racing
// loser sees zero rows affected only once the winner's row is committed and
// readable.
func (r *ReleaseCheckRepository) Create(ctx context.Context, check *releasecheck.Check) (bool, error) {
	args, err := encodeCheck(check)
	if err != nil {
		return false, err
	}
	res, err := r.q.ExecContext(ctx,
		`INSERT INTO release_checks (`+checkColumns+`)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32,$33)
		 ON CONFLICT (check_id) DO NOTHING`,
		args...)
	if err != nil {
		return false, fmt.Errorf("insert check: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("insert check rows affected: %w", err)
	}
	return n == 1, nil
}

// encodeCheck serializes check into the positional arguments of checkColumns.
func encodeCheck(check *releasecheck.Check) ([]any, error) {
	imageTagsJSON, err := json.Marshal(check.ImageTags())
	if err != nil {
		return nil, fmt.Errorf("marshal image_tags: %w", err)
	}
	transitionsJSON, err := json.Marshal(serialization.TransitionsFromDomain(check.Transitions()))
	if err != nil {
		return nil, fmt.Errorf("marshal transitions: %w", err)
	}
	perNodeJSON, err := json.Marshal(serialization.NodeValidationResultsFromDomain(check.PerNodeResults()))
	if err != nil {
		return nil, fmt.Errorf("marshal per_node_results: %w", err)
	}
	noticesJSON, err := json.Marshal(serialization.NoticesFromDomain(check.Notices()))
	if err != nil {
		return nil, fmt.Errorf("marshal notices: %w", err)
	}
	failReason := sql.NullString{String: check.FailReason(), Valid: check.FailReason() != ""}
	// A nil payload must round-trip as SQL NULL, not an empty jsonb value.
	var rejectionPayload []byte
	if p := check.RejectionPayload(); len(p) > 0 {
		rejectionPayload = p
	}
	// A check without an artifact writes NULLs, so "has no reference" is one SQL
	// predicate (candidate_topology_uri IS NULL) rather than an empty-string check.
	ref := check.CandidateTopologyRef()
	topologyURI := sql.NullString{String: ref.URI, Valid: !ref.IsZero()}
	topologySHA256 := sql.NullString{String: ref.SHA256, Valid: !ref.IsZero()}
	var nodeCount any
	if !ref.IsZero() {
		nodeCount = ref.NodeCount
	}
	bundle := check.CandidateContractBundle()
	// A timing the parse did not report is SQL NULL, so "reported" is one SQL
	// predicate (parse_ms IS NOT NULL), and an empty count is the object {}.
	timings := check.ParseTimings()
	var parseMs, contractsMs sql.NullInt64
	if timings.ParseMs != nil {
		parseMs = sql.NullInt64{Int64: *timings.ParseMs, Valid: true}
	}
	if timings.ContractsMs != nil {
		contractsMs = sql.NullInt64{Int64: *timings.ContractsMs, Valid: true}
	}
	var contractsByFamily []byte
	if timings.ContractsByFamily != nil {
		contractsByFamily, err = json.Marshal(timings.ContractsByFamily)
		if err != nil {
			return nil, fmt.Errorf("marshal contracts_by_family: %w", err)
		}
	}
	return []any{
		check.ID(), string(check.Kind()), string(check.Status()), imageTagsJSON, check.ChangedProject(),
		pq.StringArray(check.ValidationNodeIDs()), failReason, check.FailDetail(), pq.StringArray(check.FailingNodes()),
		perNodeJSON, check.CreatedAt(), transitionsJSON, check.CodeBundleURI(), string(check.ManifestKind()),
		check.IsBootstrap(), check.Repo(), check.CommitSHA(), max(check.RemediationRound(), 1), rejectionPayload,
		check.VerifiesReleaseID(), check.Try(), check.SourceOverlayURI(),
		topologyURI, topologySHA256, nodeCount, check.IsRebaseline(), noticesJSON,
		bundle.URI, bundle.SHA256, check.ContinuoContracts(),
		parseMs, contractsMs, contractsByFamily,
	}, nil
}

func rowToCheck(row checkRow) (*releasecheck.Check, error) {
	imageTags := map[string]string{}
	if len(row.ImageTagsJSON) > 0 {
		if err := json.Unmarshal(row.ImageTagsJSON, &imageTags); err != nil {
			return nil, fmt.Errorf("unmarshal image_tags: %w", err)
		}
	}
	var perNode []releasecheck.NodeValidationResult
	if len(row.PerNodeResults) > 0 {
		var perNodeDTO []serialization.NodeValidationResultDTO
		if err := json.Unmarshal(row.PerNodeResults, &perNodeDTO); err != nil {
			return nil, fmt.Errorf("unmarshal per_node_results: %w", err)
		}
		perNode = serialization.NodeValidationResultsToDomain(perNodeDTO)
	}
	var transitions []releasecheck.Transition
	if len(row.TransitionsJSON) > 0 {
		var transitionsDTO []serialization.TransitionDTO
		if err := json.Unmarshal(row.TransitionsJSON, &transitionsDTO); err != nil {
			return nil, fmt.Errorf("unmarshal transitions: %w", err)
		}
		transitions = serialization.TransitionsToDomain(transitionsDTO)
	}
	var notices []release.Notice
	if len(row.Notices) > 0 {
		var noticesDTO []serialization.NoticeDTO
		if err := json.Unmarshal(row.Notices, &noticesDTO); err != nil {
			return nil, fmt.Errorf("unmarshal notices: %w", err)
		}
		notices = serialization.NoticesToDomain(noticesDTO)
	}
	timings := release.ParseTimings{}
	// The two durations are set together or not at all, as ParseTimings holds
	// them: a row with only one of them reports neither.
	if row.ParseMs.Valid && row.ContractsMs.Valid {
		timings.ParseMs = &row.ParseMs.Int64
		timings.ContractsMs = &row.ContractsMs.Int64
	}
	if row.ContractsByFamily != nil {
		if err := json.Unmarshal(row.ContractsByFamily, &timings.ContractsByFamily); err != nil {
			return nil, fmt.Errorf("unmarshal contracts_by_family: %w", err)
		}
	}
	return releasecheck.Rehydrate(releasecheck.RehydrateInput{
		ID:             row.CheckID,
		Kind:           releasecheck.Kind(row.CheckKind),
		Status:         releasecheck.Status(row.Status),
		ImageTags:      imageTags,
		ChangedProject: row.ChangedProject,
		ManifestKind:   release.ManifestKind(row.ManifestKind),
		CandidateTopology: release.TopologyRef{
			URI:       row.CandidateTopologyURI.String,
			SHA256:    row.CandidateTopologySHA256.String,
			NodeCount: int(row.CandidateNodeCount.Int32),
		},
		CandidateContractBundle: release.ContractBundleRef{
			URI:    row.CandidateContractBundleURI,
			SHA256: row.CandidateContractBundleSHA256,
		},
		ContinuoContracts: row.ContinuoContracts,
		ParseTimings:      timings,
		ValidationNodeIDs: []string(row.ValidationNodeIDs),
		PerNodeResults:    perNode,
		FailReason:        row.FailReason.String,
		FailDetail:        row.FailDetail,
		FailingNodes:      []string(row.FailingNodes),
		CodeBundleURI:     row.CodeBundleURI,
		CreatedAt:         row.CreatedAt.Time,
		Transitions:       transitions,
		Notices:           notices,
		Bootstrap:         row.Bootstrap,
		Rebaseline:        row.Rebaseline,
		Repo:              row.Repo,
		CommitSHA:         row.CommitSHA,
		RemediationRound:  row.RemediationRound,
		RejectionPayload:  row.RejectionPayload,
		VerifiesReleaseID: row.VerifiesReleaseID,
		Try:               row.Try,
		SourceOverlayURI:  row.SourceOverlayURI,
	}), nil
}

// List returns a page of checks newest-first, narrowed by f. The cursor is
// keyset-based (created_at, check_id) so it is stable under concurrent inserts.
func (r *ReleaseCheckRepository) List(ctx context.Context, f repository.ListFilter) ([]*releasecheck.Check, *repository.ListCursor, error) {
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
		conds = append(conds, fmt.Sprintf("check_kind = $%d", len(args)))
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
		args = append(args, f.Cursor.CreatedAt, f.Cursor.CheckID)
		conds = append(conds, fmt.Sprintf("(created_at, check_id) < ($%d, $%d)", len(args)-1, len(args)))
	}
	where := ""
	if len(conds) > 0 {
		where = "WHERE " + strings.Join(conds, " AND ")
	}
	args = append(args, limit+1)
	query := fmt.Sprintf(`SELECT `+checkColumns+` FROM release_checks %s
	 ORDER BY created_at DESC, check_id DESC LIMIT $%d`, where, len(args))
	rows, err := r.q.QueryxContext(ctx, query, args...)
	if err != nil {
		return nil, nil, fmt.Errorf("list checks: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []*releasecheck.Check
	for rows.Next() {
		var row checkRow
		if err := rows.StructScan(&row); err != nil {
			return nil, nil, fmt.Errorf("scan check row: %w", err)
		}
		check, err := rowToCheck(row)
		if err != nil {
			return nil, nil, err
		}
		out = append(out, check)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("iterate check rows: %w", err)
	}
	var next *repository.ListCursor
	if len(out) > limit {
		last := out[limit-1]
		next = &repository.ListCursor{CreatedAt: last.CreatedAt(), CheckID: last.ID()}
		out = out[:limit]
	}
	return out, next, nil
}

// DeleteFinishedBefore removes terminal checks of either kind created before
// cutoff and not in keepIDs. For each removed check it then deletes the
// candidate-SQL and code-bundle prefixes from S3, plus the topology artifact
// and the contract bundle of a check that was never promoted. A promoted check's
// artifact and contract bundle are kept: they record a topology and contracts
// that ran in production. S3 deletion soft-fails: an object
// whose deletion fails stays in the bucket, and only the candidate SQL has a
// bucket lifecycle rule behind it (an age rule on the code bundles could delete
// the bundle of the live release, so the bundles have none).
// Runs inside the caller's transaction; see the prune handler.
func (r *ReleaseCheckRepository) DeleteFinishedBefore(ctx context.Context, cutoff time.Time, keepIDs []string) (int, error) {
	rows, err := r.q.QueryxContext(ctx,
		`DELETE FROM release_checks
		 WHERE status IN `+terminalStatuses+`
		   AND created_at < $1
		   AND check_id <> ALL($2)
		 RETURNING check_id, status`,
		cutoff, pq.Array(keepIDs))
	if err != nil {
		return 0, fmt.Errorf("delete finished checks: %w", err)
	}
	defer func() { _ = rows.Close() }()
	type prunedCheck struct{ id, status string }
	var pruned []prunedCheck
	for rows.Next() {
		var p prunedCheck
		if err := rows.Scan(&p.id, &p.status); err != nil {
			return 0, fmt.Errorf("scan deleted check: %w", err)
		}
		pruned = append(pruned, p)
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("iterate deleted checks: %w", err)
	}
	if r.deleter != nil {
		for _, p := range pruned {
			_ = r.deleter.DeletePrefix(ctx, "candidate-sql/"+p.id+"/")
			_ = r.deleter.DeletePrefix(ctx, "code-bundles/"+p.id+"/")
			if p.status != string(releasecheck.StatusPromoted) {
				_ = r.deleter.DeletePrefix(ctx, topologyartifact.Key(pkgevents.DefaultTenantID, p.id))
				_ = r.deleter.DeletePrefix(ctx, contractbundle.Key(pkgevents.DefaultTenantID, p.id))
			}
		}
	}
	return len(pruned), nil
}
