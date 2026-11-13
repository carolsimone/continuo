package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"

	"github.com/carolsimone/continuo/agent-remediation/domain/proposal"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/domain/repository"
	"github.com/carolsimone/continuo/agent-remediation/domain/workflow"
)

// RemediationRepository is the Postgres-backed RemediationRepository port. Bound
// to a transaction it is the repository of a step's commit; bound to the
// database it serves the claims, the lists and the retention delete.
type RemediationRepository struct {
	q Queryer
}

var _ repository.RemediationRepository = (*RemediationRepository)(nil)

// NewRemediationRepository binds a repository to a Queryer: a *sqlx.Tx for the
// write path of a unit of work, a *sqlx.DB for the calls that run on their own.
func NewRemediationRepository(q Queryer) *RemediationRepository {
	return &RemediationRepository{q: q}
}

// remediationRow is the persistence DTO of a remediation row. Its db tags live
// here so the domain type carries no storage annotations.
type remediationRow struct {
	ID               uuid.UUID      `db:"id"`
	TenantID         string         `db:"tenant_id"`
	ReleaseID        string         `db:"release_id"`
	Round            int            `db:"remediation_round"`
	Flow             string         `db:"flow"`
	Status           string         `db:"status"`
	TriggerPayload   []byte         `db:"trigger_payload"`
	TriggerMessageID string         `db:"trigger_message_id"`
	TriggerFields    []byte         `db:"trigger_fields"`
	CurrentTry       int            `db:"current_try"`
	LeaseOwner       sql.NullString `db:"lease_owner"`
	LeaseUntil       *time.Time     `db:"lease_until"`
	Failure          string         `db:"failure"`
	CreatedAt        time.Time      `db:"created_at"`
	UpdatedAt        time.Time      `db:"updated_at"`
}

const remediationColumns = `id, tenant_id, release_id, remediation_round, flow, status, trigger_payload,
	trigger_message_id, trigger_fields, current_try, lease_owner, lease_until, failure, created_at, updated_at`

func (row remediationRow) toDomain() (remediation.Remediation, error) {
	var fields map[string]string
	if err := json.Unmarshal(row.TriggerFields, &fields); err != nil {
		return remediation.Remediation{}, fmt.Errorf("decode trigger fields of remediation %s: %w", row.ID, err)
	}
	if len(fields) == 0 {
		fields = nil
	}
	return remediation.Remediation{
		ID:               row.ID,
		TenantID:         row.TenantID,
		ReleaseID:        row.ReleaseID,
		Round:            row.Round,
		Flow:             row.Flow,
		Status:           remediation.RemediationStatus(row.Status),
		TriggerPayload:   row.TriggerPayload,
		TriggerMessageID: row.TriggerMessageID,
		TriggerFields:    fields,
		CurrentTry:       row.CurrentTry,
		LeaseOwner:       row.LeaseOwner.String,
		LeaseUntil:       row.LeaseUntil,
		Failure:          row.Failure,
		CreatedAt:        row.CreatedAt,
		UpdatedAt:        row.UpdatedAt,
	}, nil
}

// tryRow is the persistence DTO of a remediation_try row.
type tryRow struct {
	RemediationID uuid.UUID `db:"remediation_id"`
	Try           int       `db:"try"`
	Attempt       int       `db:"attempt"`
	Status        string    `db:"status"`
	FileRevisions []byte    `db:"file_revisions"`
	DeadlineAt    time.Time `db:"deadline_at"`
	CreatedAt     time.Time `db:"created_at"`
	UpdatedAt     time.Time `db:"updated_at"`
}

const tryColumns = `remediation_id, try, attempt, status, file_revisions, deadline_at, created_at, updated_at`

func (row tryRow) toDomain() (remediation.Try, error) {
	var revisions map[string]string
	if err := json.Unmarshal(row.FileRevisions, &revisions); err != nil {
		return remediation.Try{}, fmt.Errorf("decode file revisions of try %d of remediation %s: %w", row.Try, row.RemediationID, err)
	}
	if len(revisions) == 0 {
		revisions = nil
	}
	return remediation.Try{
		RemediationID: row.RemediationID,
		Try:           row.Try,
		Attempt:       row.Attempt,
		Status:        remediation.TryStatus(row.Status),
		FileRevisions: revisions,
		DeadlineAt:    row.DeadlineAt,
		CreatedAt:     row.CreatedAt,
		UpdatedAt:     row.UpdatedAt,
	}, nil
}

// clusterRow is the persistence DTO of a remediation_cluster row.
type clusterRow struct {
	RemediationID uuid.UUID      `db:"remediation_id"`
	Try           int            `db:"try"`
	Cluster       int            `db:"cluster"`
	Kind          string         `db:"kind"`
	TargetNodeID  string         `db:"target_node_id"`
	Members       pq.StringArray `db:"members"`
	TypeName      string         `db:"type_name"`
	Status        string         `db:"status"`
	Round         int            `db:"round"`
	Outcome       []byte         `db:"outcome"`
	UpdatedAt     time.Time      `db:"updated_at"`
}

const clusterColumns = `remediation_id, try, cluster, kind, target_node_id, members, type_name, status, round, outcome, updated_at`

func (row clusterRow) toDomain() (remediation.Cluster, error) {
	var outcome *proposal.NodeOutcome
	if len(row.Outcome) > 0 {
		var o nodeOutcomeRow
		if err := json.Unmarshal(row.Outcome, &o); err != nil {
			return remediation.Cluster{}, fmt.Errorf("decode outcome of cluster %d of remediation %s: %w", row.Cluster, row.RemediationID, err)
		}
		outcome = &proposal.NodeOutcome{Status: proposal.Status(o.Status), Reason: o.Reason}
	}
	var members []string
	if len(row.Members) > 0 {
		members = []string(row.Members)
	}
	return remediation.Cluster{
		RemediationID: row.RemediationID,
		Try:           row.Try,
		Cluster:       row.Cluster,
		Kind:          workflow.ClusterKind(row.Kind),
		TargetNodeID:  row.TargetNodeID,
		Members:       members,
		TypeName:      row.TypeName,
		Status:        remediation.ClusterStatus(row.Status),
		Round:         row.Round,
		Outcome:       outcome,
		UpdatedAt:     row.UpdatedAt,
	}, nil
}

// findingRow is the persistence DTO of one element of a step's findings column.
type findingRow struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	File    string `json:"file,omitempty"`
	Line    int    `json:"line,omitempty"`
}

// stepRow is the persistence DTO of a remediation_step row.
type stepRow struct {
	ID            int64      `db:"id"`
	RemediationID uuid.UUID  `db:"remediation_id"`
	Try           int        `db:"try"`
	Cluster       int        `db:"cluster"`
	Step          string     `db:"step"`
	Kind          string     `db:"kind"`
	Round         int        `db:"round"`
	Status        string     `db:"status"`
	InputDigest   string     `db:"input_digest"`
	Output        []byte     `db:"output"`
	Findings      []byte     `db:"findings"`
	WaitingOn     []byte     `db:"waiting_on"`
	Model         string     `db:"model"`
	PromptVersion string     `db:"prompt_version"`
	TokensIn      int        `db:"tokens_in"`
	TokensOut     int        `db:"tokens_out"`
	CacheHit      bool       `db:"cache_hit"`
	FileRevision  string     `db:"file_revision"`
	Failures      int        `db:"failures"`
	LastError     string     `db:"last_error"`
	StartedAt     time.Time  `db:"started_at"`
	FinishedAt    *time.Time `db:"finished_at"`
}

const stepColumns = `id, remediation_id, try, cluster, step, kind, round, status, input_digest, output, findings,
	waiting_on, model, prompt_version, tokens_in, tokens_out, cache_hit, file_revision, failures, last_error,
	started_at, finished_at`

func (row stepRow) toDomain() (remediation.StepRun, error) {
	var findings []findingRow
	if err := json.Unmarshal(row.Findings, &findings); err != nil {
		return remediation.StepRun{}, fmt.Errorf("decode findings of step %d: %w", row.ID, err)
	}
	var domainFindings []remediation.Finding
	for _, f := range findings {
		domainFindings = append(domainFindings, remediation.Finding{Code: f.Code, Message: f.Message, File: f.File, Line: f.Line})
	}
	var waitingOn []string
	if len(row.WaitingOn) > 0 {
		if err := json.Unmarshal(row.WaitingOn, &waitingOn); err != nil {
			return remediation.StepRun{}, fmt.Errorf("decode waiting_on of step %d: %w", row.ID, err)
		}
	}
	if len(waitingOn) == 0 {
		waitingOn = nil
	}
	return remediation.StepRun{
		ID:            row.ID,
		RemediationID: row.RemediationID,
		Try:           row.Try,
		Cluster:       row.Cluster,
		Step:          row.Step,
		Kind:          row.Kind,
		Round:         row.Round,
		Status:        remediation.StepStatus(row.Status),
		InputDigest:   row.InputDigest,
		Output:        row.Output,
		Findings:      domainFindings,
		WaitingOn:     waitingOn,
		Model:         row.Model,
		PromptVersion: row.PromptVersion,
		TokensIn:      row.TokensIn,
		TokensOut:     row.TokensOut,
		CacheHit:      row.CacheHit,
		FileRevision:  row.FileRevision,
		Failures:      row.Failures,
		LastError:     row.LastError,
		StartedAt:     row.StartedAt,
		FinishedAt:    row.FinishedAt,
	}, nil
}

// timeArg is a timestamp column value that defaults to the database clock: NULL
// for a zero time, which the statements COALESCE with now().
func timeArg(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t
}

// limitArg is a LIMIT value: NULL (no limit) when limit is not positive.
func limitArg(limit int) any {
	if limit <= 0 {
		return nil
	}
	return limit
}

// encodeStringMap encodes m as a JSON object; a nil or empty map is nil.
func encodeStringMap(m map[string]string) ([]byte, error) {
	if len(m) == 0 {
		return nil, nil
	}
	return json.Marshal(m)
}

// jsonOr returns raw, or empty when raw holds nothing, so a NOT NULL JSONB
// column holds an empty document instead of SQL NULL.
func jsonOr(raw []byte, empty string) []byte {
	if len(raw) == 0 {
		return []byte(empty)
	}
	return raw
}

// Create inserts r on the natural key (tenant, release, round). A remediation
// that exists for the key is left untouched and created is false.
func (r *RemediationRepository) Create(ctx context.Context, rem remediation.Remediation) (bool, error) {
	if rem.ID == uuid.Nil {
		return false, errors.New("create remediation: the id is unset")
	}
	fields, err := encodeStringMap(rem.TriggerFields)
	if err != nil {
		return false, fmt.Errorf("encode trigger fields: %w", err)
	}
	var leaseOwner any
	if rem.LeaseOwner != "" {
		leaseOwner = rem.LeaseOwner
	}
	var leaseUntil any
	if rem.LeaseUntil != nil {
		leaseUntil = *rem.LeaseUntil
	}
	res, err := r.q.ExecContext(ctx, `
		INSERT INTO remediation
			(id, tenant_id, release_id, remediation_round, flow, status, trigger_payload,
			 trigger_message_id, trigger_fields, current_try, lease_owner, lease_until, failure,
			 created_at, updated_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,
		        COALESCE($14::timestamptz, now()), COALESCE($15::timestamptz, now()))
		ON CONFLICT (tenant_id, release_id, remediation_round) DO NOTHING`,
		rem.ID, rem.TenantID, rem.ReleaseID, rem.Round, rem.Flow, rem.Status, jsonOr(rem.TriggerPayload, "{}"),
		rem.TriggerMessageID, jsonOr(fields, "{}"), rem.CurrentTry, leaseOwner, leaseUntil, rem.Failure,
		timeArg(rem.CreatedAt), timeArg(rem.UpdatedAt))
	if err != nil {
		return false, fmt.Errorf("create remediation: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("create remediation: rows affected: %w", err)
	}
	return n > 0, nil
}

// FindByRelease returns the remediation of one tenant, release and round.
func (r *RemediationRepository) FindByRelease(ctx context.Context, tenantID, releaseID string, round int) (remediation.Remediation, bool, error) {
	var row remediationRow
	err := r.q.GetContext(ctx, &row, `SELECT `+remediationColumns+`
		FROM remediation WHERE tenant_id=$1 AND release_id=$2 AND remediation_round=$3`,
		tenantID, releaseID, round)
	if errors.Is(err, sql.ErrNoRows) {
		return remediation.Remediation{}, false, nil
	}
	if err != nil {
		return remediation.Remediation{}, false, fmt.Errorf("find remediation by release: %w", err)
	}
	rem, err := row.toDomain()
	if err != nil {
		return remediation.Remediation{}, false, err
	}
	return rem, true, nil
}

// Load returns the remediation with its current try, and that try's clusters
// and steps. It errors when current_try names a try that has no row. The reads
// are separate statements, so a Load is consistent only while no one else
// writes the remediation: the holder of its lease can rely on it, a caller
// without the lease (ListVerifying's snapshots) may read a torn state and acts
// only after it Claims the remediation and Loads again.
func (r *RemediationRepository) Load(ctx context.Context, id uuid.UUID) (remediation.Snapshot, error) {
	var row remediationRow
	err := r.q.GetContext(ctx, &row, `SELECT `+remediationColumns+` FROM remediation WHERE id=$1`, id)
	if errors.Is(err, sql.ErrNoRows) {
		return remediation.Snapshot{}, repository.ErrRemediationNotFound
	}
	if err != nil {
		return remediation.Snapshot{}, fmt.Errorf("load remediation: %w", err)
	}
	rem, err := row.toDomain()
	if err != nil {
		return remediation.Snapshot{}, err
	}
	snap := remediation.Snapshot{Remediation: rem}
	if rem.CurrentTry == 0 {
		return snap, nil
	}

	var try tryRow
	err = r.q.GetContext(ctx, &try, `SELECT `+tryColumns+` FROM remediation_try WHERE remediation_id=$1 AND try=$2`,
		id, rem.CurrentTry)
	if errors.Is(err, sql.ErrNoRows) {
		return remediation.Snapshot{}, fmt.Errorf("load remediation %s: current try %d has no row", id, rem.CurrentTry)
	}
	if err != nil {
		return remediation.Snapshot{}, fmt.Errorf("load remediation try: %w", err)
	}
	domainTry, err := try.toDomain()
	if err != nil {
		return remediation.Snapshot{}, err
	}
	snap.Try = &domainTry

	var clusters []clusterRow
	if err := r.q.SelectContext(ctx, &clusters, `SELECT `+clusterColumns+`
		FROM remediation_cluster WHERE remediation_id=$1 AND try=$2 ORDER BY cluster`, id, rem.CurrentTry); err != nil {
		return remediation.Snapshot{}, fmt.Errorf("load remediation clusters: %w", err)
	}
	for _, c := range clusters {
		domainCluster, err := c.toDomain()
		if err != nil {
			return remediation.Snapshot{}, err
		}
		snap.Clusters = append(snap.Clusters, domainCluster)
	}

	var steps []stepRow
	if err := r.q.SelectContext(ctx, &steps, `SELECT `+stepColumns+`
		FROM remediation_step WHERE remediation_id=$1 AND try=$2 ORDER BY id`, id, rem.CurrentTry); err != nil {
		return remediation.Snapshot{}, fmt.Errorf("load remediation steps: %w", err)
	}
	for _, s := range steps {
		domainStep, err := s.toDomain()
		if err != nil {
			return remediation.Snapshot{}, err
		}
		snap.Steps = append(snap.Steps, domainStep)
	}
	return snap, nil
}

// claimNextStatement leases the oldest claimable running remediation in one
// statement. The inner SELECT locks the row and skips rows another claim holds,
// so concurrent callers take different rows, and the UPDATE needs no
// surrounding transaction. A remediation whose current try is verifying is
// left to the verification poller.
const claimNextStatement = `
	UPDATE remediation SET lease_owner = $1, lease_until = $2
	 WHERE id = (SELECT r.id FROM remediation r
	              WHERE r.status = 'running'
	                AND (r.lease_until IS NULL OR r.lease_until < $3)
	                AND NOT EXISTS (SELECT 1 FROM remediation_try t
	                                 WHERE t.remediation_id = r.id AND t.try = r.current_try
	                                   AND t.status = 'verifying')
	              ORDER BY r.created_at, r.id
	                FOR UPDATE OF r SKIP LOCKED
	              LIMIT 1)
	RETURNING id`

// ClaimNext leases the oldest claimable running remediation until now+lease.
func (r *RemediationRepository) ClaimNext(ctx context.Context, owner string, now time.Time, lease time.Duration) (uuid.UUID, bool, error) {
	var id uuid.UUID
	err := r.q.GetContext(ctx, &id, claimNextStatement, owner, now.Add(lease), now)
	if errors.Is(err, sql.ErrNoRows) {
		return uuid.Nil, false, nil
	}
	if err != nil {
		return uuid.Nil, false, fmt.Errorf("claim next remediation: %w", err)
	}
	return id, true, nil
}

// Claim leases one running remediation whose lease is free or ended before now.
// The WHERE clause is the whole check: a concurrent claim holds the row lock,
// and once it commits this statement re-reads the row and matches nothing.
func (r *RemediationRepository) Claim(ctx context.Context, id uuid.UUID, owner string, now time.Time, lease time.Duration) (bool, error) {
	res, err := r.q.ExecContext(ctx, `
		UPDATE remediation SET lease_owner = $2, lease_until = $3
		 WHERE id = $1 AND status = 'running' AND (lease_until IS NULL OR lease_until < $4)`,
		id, owner, now.Add(lease), now)
	if err != nil {
		return false, fmt.Errorf("claim remediation: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("claim remediation: rows affected: %w", err)
	}
	return n > 0, nil
}

// Hold extends owner's unexpired lease to now+lease, or returns ErrLeaseLost.
func (r *RemediationRepository) Hold(ctx context.Context, id uuid.UUID, owner string, now time.Time, lease time.Duration) error {
	res, err := r.q.ExecContext(ctx, `
		UPDATE remediation SET lease_until = $3, updated_at = now()
		 WHERE id = $1 AND lease_owner = $2 AND lease_until >= $4`,
		id, owner, now.Add(lease), now)
	if err != nil {
		return fmt.Errorf("hold remediation lease: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("hold remediation lease: rows affected: %w", err)
	}
	if n == 0 {
		return repository.ErrLeaseLost
	}
	return nil
}

// ReleaseLease frees the lease when owner holds it.
func (r *RemediationRepository) ReleaseLease(ctx context.Context, id uuid.UUID, owner string) error {
	if _, err := r.q.ExecContext(ctx, `
		UPDATE remediation SET lease_owner = NULL, lease_until = NULL
		 WHERE id = $1 AND lease_owner = $2`, id, owner); err != nil {
		return fmt.Errorf("release remediation lease: %w", err)
	}
	return nil
}

// SaveRemediation writes the run's own columns and leaves its lease alone.
func (r *RemediationRepository) SaveRemediation(ctx context.Context, rem remediation.Remediation) error {
	fields, err := encodeStringMap(rem.TriggerFields)
	if err != nil {
		return fmt.Errorf("encode trigger fields: %w", err)
	}
	res, err := r.q.ExecContext(ctx, `
		UPDATE remediation
		   SET flow = $2, status = $3, trigger_payload = $4, trigger_message_id = $5,
		       trigger_fields = $6, current_try = $7, failure = $8,
		       updated_at = COALESCE($9::timestamptz, now())
		 WHERE id = $1`,
		rem.ID, rem.Flow, rem.Status, jsonOr(rem.TriggerPayload, "{}"), rem.TriggerMessageID,
		jsonOr(fields, "{}"), rem.CurrentTry, rem.Failure, timeArg(rem.UpdatedAt))
	if err != nil {
		return fmt.Errorf("save remediation: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("save remediation: rows affected: %w", err)
	}
	if n == 0 {
		return fmt.Errorf("save remediation %s: %w", rem.ID, repository.ErrRemediationNotFound)
	}
	return nil
}

// SaveTry inserts the try or updates it on (remediation, try).
func (r *RemediationRepository) SaveTry(ctx context.Context, t remediation.Try) error {
	revisions, err := encodeStringMap(t.FileRevisions)
	if err != nil {
		return fmt.Errorf("encode file revisions: %w", err)
	}
	if _, err := r.q.ExecContext(ctx, `
		INSERT INTO remediation_try (remediation_id, try, attempt, status, file_revisions, deadline_at, created_at, updated_at)
		VALUES ($1,$2,$3,$4,$5,$6, COALESCE($7::timestamptz, now()), COALESCE($8::timestamptz, now()))
		ON CONFLICT (remediation_id, try) DO UPDATE SET
			attempt        = EXCLUDED.attempt,
			status         = EXCLUDED.status,
			file_revisions = EXCLUDED.file_revisions,
			deadline_at    = EXCLUDED.deadline_at,
			updated_at     = EXCLUDED.updated_at`,
		t.RemediationID, t.Try, t.Attempt, t.Status, jsonOr(revisions, "{}"), t.DeadlineAt,
		timeArg(t.CreatedAt), timeArg(t.UpdatedAt)); err != nil {
		return fmt.Errorf("save remediation try: %w", err)
	}
	return nil
}

// SaveCluster inserts the cluster or updates it on (remediation, try, cluster).
func (r *RemediationRepository) SaveCluster(ctx context.Context, c remediation.Cluster) error {
	var outcome any
	if c.Outcome != nil {
		raw, err := json.Marshal(nodeOutcomeRow{Status: string(c.Outcome.Status), Reason: c.Outcome.Reason})
		if err != nil {
			return fmt.Errorf("encode cluster outcome: %w", err)
		}
		outcome = raw
	}
	if _, err := r.q.ExecContext(ctx, `
		INSERT INTO remediation_cluster
			(remediation_id, try, cluster, kind, target_node_id, members, type_name, status, round, outcome, updated_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10, COALESCE($11::timestamptz, now()))
		ON CONFLICT (remediation_id, try, cluster) DO UPDATE SET
			kind           = EXCLUDED.kind,
			target_node_id = EXCLUDED.target_node_id,
			members        = EXCLUDED.members,
			type_name      = EXCLUDED.type_name,
			status         = EXCLUDED.status,
			round          = EXCLUDED.round,
			outcome        = EXCLUDED.outcome,
			updated_at     = EXCLUDED.updated_at`,
		c.RemediationID, c.Try, c.Cluster, c.Kind, c.TargetNodeID, textArray(c.Members), c.TypeName,
		c.Status, c.Round, outcome, timeArg(c.UpdatedAt)); err != nil {
		return fmt.Errorf("save remediation cluster: %w", err)
	}
	return nil
}

// SaveStep inserts the step run or updates it on its unique key, keeping the
// id the first insert assigned. A NUL character in the output, the findings or
// the waited-on run ids is stored as the replacement character, since a jsonb
// column refuses it.
func (r *RemediationRepository) SaveStep(ctx context.Context, s remediation.StepRun) error {
	rows := make([]findingRow, 0, len(s.Findings))
	for _, f := range s.Findings {
		rows = append(rows, findingRow{Code: f.Code, Message: f.Message, File: f.File, Line: f.Line})
	}
	findings, err := json.Marshal(rows)
	if err != nil {
		return fmt.Errorf("encode findings: %w", err)
	}
	findings = replaceNULEscapes(findings)
	var output any
	if len(s.Output) > 0 {
		output = replaceNULEscapes(s.Output)
	}
	var waitingOn any
	if len(s.WaitingOn) > 0 {
		raw, err := json.Marshal(s.WaitingOn)
		if err != nil {
			return fmt.Errorf("encode waiting_on: %w", err)
		}
		waitingOn = replaceNULEscapes(raw)
	}
	var finishedAt any
	if s.FinishedAt != nil {
		finishedAt = *s.FinishedAt
	}
	if _, err := r.q.ExecContext(ctx, `
		INSERT INTO remediation_step
			(remediation_id, try, cluster, step, kind, round, status, input_digest, output, findings,
			 waiting_on, model, prompt_version, tokens_in, tokens_out, cache_hit, file_revision,
			 failures, last_error, started_at, finished_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,
		        COALESCE($20::timestamptz, now()), $21)
		ON CONFLICT (remediation_id, try, cluster, step, round) DO UPDATE SET
			kind           = EXCLUDED.kind,
			status         = EXCLUDED.status,
			input_digest   = EXCLUDED.input_digest,
			output         = EXCLUDED.output,
			findings       = EXCLUDED.findings,
			waiting_on     = EXCLUDED.waiting_on,
			model          = EXCLUDED.model,
			prompt_version = EXCLUDED.prompt_version,
			tokens_in      = EXCLUDED.tokens_in,
			tokens_out     = EXCLUDED.tokens_out,
			cache_hit      = EXCLUDED.cache_hit,
			file_revision  = EXCLUDED.file_revision,
			failures       = EXCLUDED.failures,
			last_error     = EXCLUDED.last_error,
			started_at     = EXCLUDED.started_at,
			finished_at    = EXCLUDED.finished_at`,
		s.RemediationID, s.Try, s.Cluster, s.Step, s.Kind, s.Round, s.Status, s.InputDigest, output, findings,
		waitingOn, s.Model, s.PromptVersion, s.TokensIn, s.TokensOut, s.CacheHit, s.FileRevision,
		s.Failures, s.LastError, timeArg(s.StartedAt), finishedAt); err != nil {
		return fmt.Errorf("save remediation step: %w", err)
	}
	return nil
}

// ListVerifying returns the running remediations whose current try is
// verifying, oldest first, each loaded with its current try.
func (r *RemediationRepository) ListVerifying(ctx context.Context, limit int) ([]remediation.Snapshot, error) {
	var ids []uuid.UUID
	if err := r.q.SelectContext(ctx, &ids, `
		SELECT r.id FROM remediation r
		  JOIN remediation_try t ON t.remediation_id = r.id AND t.try = r.current_try
		 WHERE r.status = 'running' AND t.status = 'verifying'
		 ORDER BY r.created_at, r.id
		 LIMIT $1`, limitArg(limit)); err != nil {
		return nil, fmt.Errorf("list verifying remediations: %w", err)
	}
	snaps := make([]remediation.Snapshot, 0, len(ids))
	for _, id := range ids {
		snap, err := r.Load(ctx, id)
		if errors.Is(err, repository.ErrRemediationNotFound) {
			continue
		}
		if err != nil {
			return nil, err
		}
		snaps = append(snaps, snap)
	}
	return snaps, nil
}

// ListPastDeadline returns the running remediations whose current try is still
// open past its deadline, oldest deadline first.
func (r *RemediationRepository) ListPastDeadline(ctx context.Context, now time.Time, limit int) ([]uuid.UUID, error) {
	var ids []uuid.UUID
	if err := r.q.SelectContext(ctx, &ids, `
		SELECT r.id FROM remediation r
		  JOIN remediation_try t ON t.remediation_id = r.id AND t.try = r.current_try
		 WHERE r.status = 'running' AND t.status IN ('drafting','checking','verifying')
		   AND t.deadline_at < $1
		 ORDER BY t.deadline_at, r.id
		 LIMIT $2`, now, limitArg(limit)); err != nil {
		return nil, fmt.Errorf("list remediations past their deadline: %w", err)
	}
	return ids, nil
}

// DeleteFinishedBefore deletes the finished remediations last updated before
// cutoff. The foreign keys delete their tries, clusters and steps and clear the
// remediation_id of their proposals.
func (r *RemediationRepository) DeleteFinishedBefore(ctx context.Context, cutoff time.Time) (int64, error) {
	res, err := r.q.ExecContext(ctx, `
		DELETE FROM remediation WHERE status IN ('done','system_failure') AND updated_at < $1`, cutoff)
	if err != nil {
		return 0, fmt.Errorf("delete finished remediations: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("delete finished remediations: rows affected: %w", err)
	}
	return n, nil
}
