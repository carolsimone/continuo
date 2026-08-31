package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/lib/pq"

	"github.com/carolsimone/continuo/agent-remediation/domain/proposal"
	"github.com/carolsimone/continuo/agent-remediation/domain/repository"
)

// Queryer is the minimal sqlx surface satisfied by both *sqlx.DB and *sqlx.Tx.
// SelectContext is required for multi-row reads (List).
type Queryer interface {
	GetContext(ctx context.Context, dest any, query string, args ...any) error
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	SelectContext(ctx context.Context, dest any, query string, args ...any) error
}

// ProposalRepository is the Postgres-backed ProposalRepository port.
type ProposalRepository struct {
	q Queryer
	// serviceRepoPaths maps a dbt service name to its project root within the
	// repository. BeginPR uses it to bucket a claimed proposal's edits by
	// owning service, so a per-service PR claims only that service's edits and
	// the nodes it fixed.
	serviceRepoPaths map[string]string
}

var _ repository.ProposalRepository = (*ProposalRepository)(nil)
var _ repository.OpenPRLister = (*ProposalRepository)(nil)
var _ repository.OpeningLister = (*ProposalRepository)(nil)

// NewProposalRepository binds a repository to a Queryer (pass *sqlx.Tx for the
// transactional write path) and the service→repo-path prefixes used to split a
// proposal's edits by owning service when claiming a per-service pull request.
func NewProposalRepository(q Queryer, serviceRepoPaths map[string]string) *ProposalRepository {
	return &ProposalRepository{q: q, serviceRepoPaths: serviceRepoPaths}
}

// CountAttempts returns the number of TERMINAL proposal attempts recorded for
// one remediation round of one release. In-flight rows — 'generating' (the
// model call has not resolved) and 'verifying' (a shadow release is still
// validating a proposed fix) — are excluded so an attempt that has not yet
// concluded neither inflates the attempt cap nor shifts the attempt number on
// a redelivery.
func (r *ProposalRepository) CountAttempts(ctx context.Context, releaseID string, remediationRound int) (int, error) {
	const query = `SELECT count(*) FROM proposal
		WHERE release_id=$1 AND remediation_round=$2
		  AND status NOT IN ('generating','verifying')`
	var count int
	if err := r.q.GetContext(ctx, &count, query, releaseID, remediationRound); err != nil {
		return 0, fmt.Errorf("count proposal attempts: %w", err)
	}
	return count, nil
}

// roundOrDefault returns n, or 1 when n is not a positive round number. A
// proposal built without an explicit remediation round (e.g. an instant
// escalation path that never saw a trigger) belongs to round 1, the round
// every proposal predating this column belongs to.
func roundOrDefault(n int) int {
	if n < 1 {
		return 1
	}
	return n
}

// InsertGenerating persists an in-flight 'generating' row for the attempt right
// before the model is called. It is idempotent: ON CONFLICT on the natural key
// (release_id, attempt) DO NOTHING, so a redelivery that re-runs the same
// attempt leaves the single generating row untouched. Only the identity +
// status columns, plus the resolved nodes and their initial outcomes, are
// written; the remaining columns take their defaults and are populated when
// Upsert finalizes the row.
func (r *ProposalRepository) InsertGenerating(ctx context.Context, p proposal.Proposal) error {
	const stmt = `
		INSERT INTO proposal
			(source, release_id, remediation_round, node_id, error_signature, attempt, status, created_at,
			 resolved_node_ids, node_outcomes)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
		ON CONFLICT (release_id, attempt) DO NOTHING`
	resolved, err := marshalResolved(p.ResolvedNodeIDs)
	if err != nil {
		return fmt.Errorf("marshal proposal resolved node ids: %w", err)
	}
	outcomes, err := marshalNodeOutcomes(p.NodeOutcomes)
	if err != nil {
		return fmt.Errorf("marshal proposal node outcomes: %w", err)
	}
	if _, err := r.q.ExecContext(ctx, stmt,
		p.Source, p.ReleaseID, roundOrDefault(p.RemediationRound), p.NodeID, p.ErrorSignature, p.Attempt,
		proposal.StatusGenerating, p.CreatedAt,
		resolved, outcomes,
	); err != nil {
		return fmt.Errorf("insert generating proposal: %w", err)
	}
	return nil
}

// FailGenerating finalizes releaseID's in-flight 'generating' row, recording
// reason as the rationale, and returns how many rows moved.
//
// One attempt now addresses a release's whole failing set, so at most one row
// per release can be generating at a time: the release id alone identifies
// the row unambiguously, with no node or error-signature match needed.
//
// The status filter is what makes it safe to run at any time: a row that has
// already reached a terminal state is left exactly as it is.
func (r *ProposalRepository) FailGenerating(ctx context.Context, releaseID, reason string) (int, error) {
	res, err := r.q.ExecContext(ctx,
		`UPDATE proposal SET status=$3, rationale=$2 WHERE release_id=$1 AND status=$4`,
		releaseID, reason, proposal.StatusFailed, proposal.StatusGenerating)
	if err != nil {
		return 0, fmt.Errorf("fail generating proposals: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("fail generating proposals: rows affected: %w", err)
	}
	return int(n), nil
}

// Upsert records the terminal outcome of a proposal attempt on the natural key
// (release_id, attempt): when an in-flight generating row exists (the common
// healable path) it is finalized in place via ON CONFLICT … DO UPDATE;
// otherwise the row is plain-inserted (instant paths — e.g. attempt-cap
// escalation — that never marked generating).
func (r *ProposalRepository) Upsert(ctx context.Context, p proposal.Proposal) error {
	const stmt = `
		INSERT INTO proposal
			(source, release_id, remediation_round, node_id, error_signature, attempt,
			 status, shadow_release_id, trigger_payload,
			 confidence, rationale, proposed_sql_uri, diff_uri,
			 candidate_fix_sql_uri, candidate_fix_diff_uri, source_resolved,
			 model, created_at,
			 repo, commit_sha, file_path, file_edits,
			 resolved_node_ids, node_outcomes, verifications)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25)
		ON CONFLICT (release_id, attempt) DO UPDATE SET
			status                 = EXCLUDED.status,
			shadow_release_id      = EXCLUDED.shadow_release_id,
			trigger_payload        = EXCLUDED.trigger_payload,
			confidence             = EXCLUDED.confidence,
			rationale              = EXCLUDED.rationale,
			proposed_sql_uri       = EXCLUDED.proposed_sql_uri,
			diff_uri               = EXCLUDED.diff_uri,
			candidate_fix_sql_uri  = EXCLUDED.candidate_fix_sql_uri,
			candidate_fix_diff_uri = EXCLUDED.candidate_fix_diff_uri,
			source_resolved        = EXCLUDED.source_resolved,
			model                  = EXCLUDED.model,
			created_at             = EXCLUDED.created_at,
			repo                   = EXCLUDED.repo,
			commit_sha             = EXCLUDED.commit_sha,
			file_path              = EXCLUDED.file_path,
			file_edits             = EXCLUDED.file_edits,
			resolved_node_ids      = EXCLUDED.resolved_node_ids,
			node_outcomes          = EXCLUDED.node_outcomes,
			verifications          = EXCLUDED.verifications`
	edits, err := marshalFileEdits(p.Edits)
	if err != nil {
		return fmt.Errorf("marshal proposal file edits: %w", err)
	}
	resolved, err := marshalResolved(p.ResolvedNodeIDs)
	if err != nil {
		return fmt.Errorf("marshal proposal resolved node ids: %w", err)
	}
	outcomes, err := marshalNodeOutcomes(p.NodeOutcomes)
	if err != nil {
		return fmt.Errorf("marshal proposal node outcomes: %w", err)
	}
	verifications, err := marshalVerifications(p.Verifications)
	if err != nil {
		return fmt.Errorf("marshal proposal verifications: %w", err)
	}
	if _, err := r.q.ExecContext(ctx, stmt,
		p.Source, p.ReleaseID, roundOrDefault(p.RemediationRound), p.NodeID, p.ErrorSignature, p.Attempt,
		p.Status, p.ShadowReleaseID, triggerPayloadOrDefault(p.TriggerPayload),
		p.Confidence, p.Rationale, p.ProposedSQLURI, p.DiffURI,
		p.CandidateFixSQLURI, p.CandidateFixDiffURI, p.SourceResolved,
		p.Model, p.CreatedAt,
		p.Repo, p.CommitSHA, p.FilePath, edits,
		resolved, outcomes, verifications,
	); err != nil {
		return fmt.Errorf("insert proposal: %w", err)
	}
	return nil
}

// triggerPayloadOrDefault returns the raw trigger_payload bytes to write,
// defaulting to an empty JSON object when the proposal carries none so the
// column (NOT NULL) is never written as SQL NULL — the same defaulting
// marshalFileEdits applies to the file_edits column.
func triggerPayloadOrDefault(raw []byte) []byte {
	if len(raw) == 0 {
		return []byte("{}")
	}
	return raw
}

// proposalRow is the persistence DTO for a full proposal projection. The db
// tags (a persistence concern) live here in the adapter so the domain
// proposal.View stays free of storage annotations.
type proposalRow struct {
	ID                  string     `db:"id"`
	Source              string     `db:"source"`
	ReleaseID           string     `db:"release_id"`
	RemediationRound    int        `db:"remediation_round"`
	NodeID              string     `db:"node_id"`
	ErrorSignature      string     `db:"error_signature"`
	Attempt             int        `db:"attempt"`
	Status              string     `db:"status"`
	ShadowReleaseID     string     `db:"shadow_release_id"`
	VerifyError         string     `db:"verify_error"`
	TriggerPayload      []byte     `db:"trigger_payload"`
	Confidence          string     `db:"confidence"`
	Rationale           string     `db:"rationale"`
	ProposedSQLURI      string     `db:"proposed_sql_uri"`
	DiffURI             string     `db:"diff_uri"`
	CandidateFixSQLURI  string     `db:"candidate_fix_sql_uri"`
	CandidateFixDiffURI string     `db:"candidate_fix_diff_uri"`
	SourceResolved      bool       `db:"source_resolved"`
	Repo                string     `db:"repo"`
	CommitSHA           string     `db:"commit_sha"`
	FilePath            string     `db:"file_path"`
	FileEdits           []byte     `db:"file_edits"`
	ResolvedNodeIDs     []byte     `db:"resolved_node_ids"`
	NodeOutcomes        []byte     `db:"node_outcomes"`
	Verifications       []byte     `db:"verifications"`
	Model               string     `db:"model"`
	CreatedAt           time.Time  `db:"created_at"`
	PrURL               string     `db:"pr_url"`
	PrNumber            int        `db:"pr_number"`
	PrState             string     `db:"pr_state"`
	PrOpenedAt          *time.Time `db:"pr_opened_at"`
	PrOpenedBy          string     `db:"pr_opened_by"`
	PrClosedAt          *time.Time `db:"pr_closed_at"`
}

// fileEditRow is the persistence DTO for one element of the file_edits JSONB
// column. The json tags — a storage concern — live here in the adapter so the
// domain proposal.FileEdit carries no serialization annotations. The stored
// shape is
// [{"path","content_uri","diff_uri","target_node_id","member_node_ids"}, ...].
// member_node_ids records the failing nodes an edit's cluster resolves, so a
// per-service pull request claimed later from this row can narrow itself to the
// members its own edits address.
type fileEditRow struct {
	Path          string   `json:"path"`
	ContentURI    string   `json:"content_uri"`
	DiffURI       string   `json:"diff_uri"`
	TargetNodeID  string   `json:"target_node_id,omitempty"`
	MemberNodeIDs []string `json:"member_node_ids,omitempty"`
}

// marshalFileEdits encodes the domain edits as the file_edits column value.
// A nil or empty slice encodes as an empty JSON array, so the column is never
// written as SQL NULL.
func marshalFileEdits(edits []proposal.FileEdit) ([]byte, error) {
	rows := make([]fileEditRow, 0, len(edits))
	for _, e := range edits {
		rows = append(rows, fileEditRow{
			Path:          e.Path,
			ContentURI:    e.ContentURI,
			DiffURI:       e.DiffURI,
			TargetNodeID:  e.TargetNodeID,
			MemberNodeIDs: e.MemberNodeIDs,
		})
	}
	return json.Marshal(rows)
}

// editsOrLegacy decodes the file_edits JSONB column into a []proposal.FileEdit.
// A malformed blob is treated the same as an empty array rather than failing
// the read: a proposal row must stay readable even if its file_edits value
// were ever corrupted, since the single-file scalar columns still carry the
// same information for a one-file proposal. When the decoded (or defaulted)
// list is empty and filePath is non-empty, one edit is synthesized from those
// scalar columns (filePath, contentURI, diffURI), which is how a row written
// before the file_edits column existed keeps reading as a single file change.
func editsOrLegacy(raw []byte, filePath, contentURI, diffURI string) []proposal.FileEdit {
	var rows []fileEditRow
	_ = json.Unmarshal(raw, &rows)
	if len(rows) == 0 && filePath != "" {
		return []proposal.FileEdit{{Path: filePath, ContentURI: contentURI, DiffURI: diffURI}}
	}
	edits := make([]proposal.FileEdit, 0, len(rows))
	for _, r := range rows {
		edits = append(edits, proposal.FileEdit{
			Path:          r.Path,
			ContentURI:    r.ContentURI,
			DiffURI:       r.DiffURI,
			TargetNodeID:  r.TargetNodeID,
			MemberNodeIDs: r.MemberNodeIDs,
		})
	}
	return edits
}

// nodeOutcomeRow is the persistence DTO for one entry of the node_outcomes
// JSONB column. The json tags live here so the domain proposal.NodeOutcome
// carries no serialization annotations.
type nodeOutcomeRow struct {
	Status string `json:"status"`
	Reason string `json:"reason,omitempty"`
}

// marshalNodeOutcomes encodes the per-node outcomes map as the node_outcomes
// column value. A nil or empty map encodes as an empty JSON object, so the
// column is never written as SQL NULL.
func marshalNodeOutcomes(m map[string]proposal.NodeOutcome) ([]byte, error) {
	rows := make(map[string]nodeOutcomeRow, len(m))
	for nodeID, o := range m {
		rows[nodeID] = nodeOutcomeRow{Status: string(o.Status), Reason: o.Reason}
	}
	return json.Marshal(rows)
}

// unmarshalNodeOutcomes decodes the node_outcomes JSONB column. A malformed
// or empty blob decodes as nil rather than an empty non-nil map, so a row
// that never recorded any per-node outcome reads back as unset.
func unmarshalNodeOutcomes(raw []byte) map[string]proposal.NodeOutcome {
	var rows map[string]nodeOutcomeRow
	_ = json.Unmarshal(raw, &rows)
	if len(rows) == 0 {
		return nil
	}
	outcomes := make(map[string]proposal.NodeOutcome, len(rows))
	for nodeID, r := range rows {
		outcomes[nodeID] = proposal.NodeOutcome{Status: proposal.Status(r.Status), Reason: r.Reason}
	}
	return outcomes
}

// verificationRow is the persistence DTO for one element of the
// verifications JSONB column. The json tags live here so the domain
// proposal.Verification carries no serialization annotations.
type verificationRow struct {
	Service         string `json:"service"`
	Kind            string `json:"kind"`
	ShadowReleaseID string `json:"shadow_release_id"`
}

// marshalVerifications encodes the domain verifications as the verifications
// column value. A nil or empty slice encodes as an empty JSON array, so the
// column is never written as SQL NULL.
func marshalVerifications(v []proposal.Verification) ([]byte, error) {
	rows := make([]verificationRow, 0, len(v))
	for _, e := range v {
		rows = append(rows, verificationRow{Service: e.Service, Kind: e.Kind, ShadowReleaseID: e.ShadowReleaseID})
	}
	return json.Marshal(rows)
}

// unmarshalVerifications decodes the verifications JSONB column. A malformed
// or empty blob decodes as nil rather than an empty non-nil slice, so a row
// that never posted a shadow release reads back as unset.
func unmarshalVerifications(raw []byte) []proposal.Verification {
	var rows []verificationRow
	_ = json.Unmarshal(raw, &rows)
	if len(rows) == 0 {
		return nil
	}
	verifications := make([]proposal.Verification, 0, len(rows))
	for _, r := range rows {
		verifications = append(verifications, proposal.Verification{Service: r.Service, Kind: r.Kind, ShadowReleaseID: r.ShadowReleaseID})
	}
	return verifications
}

// marshalResolved encodes the resolved node ids as the resolved_node_ids
// column value. A nil or empty slice encodes as an empty JSON array, so the
// column is never written as SQL NULL.
func marshalResolved(ids []string) ([]byte, error) {
	if ids == nil {
		ids = []string{}
	}
	return json.Marshal(ids)
}

// resolvedOrLegacy decodes the resolved_node_ids JSONB column into a
// []string. When the decoded (or defaulted) list is empty and nodeID is
// non-empty, one entry is synthesized from the legacy node_id column, which
// is how a row written before resolved_node_ids existed keeps reading as the
// single node it addressed.
func resolvedOrLegacy(raw []byte, nodeID string) []string {
	var ids []string
	_ = json.Unmarshal(raw, &ids)
	if len(ids) == 0 && nodeID != "" {
		return []string{nodeID}
	}
	return ids
}

func (row proposalRow) toView() proposal.View {
	return proposal.View{
		ID:                  row.ID,
		Source:              row.Source,
		ReleaseID:           row.ReleaseID,
		RemediationRound:    row.RemediationRound,
		NodeID:              row.NodeID,
		ResolvedNodeIDs:     resolvedOrLegacy(row.ResolvedNodeIDs, row.NodeID),
		ErrorSignature:      row.ErrorSignature,
		Attempt:             row.Attempt,
		Status:              proposal.Status(row.Status),
		NodeOutcomes:        unmarshalNodeOutcomes(row.NodeOutcomes),
		Verifications:       unmarshalVerifications(row.Verifications),
		ShadowReleaseID:     row.ShadowReleaseID,
		VerifyError:         row.VerifyError,
		TriggerPayload:      row.TriggerPayload,
		Confidence:          proposal.Confidence(row.Confidence),
		Rationale:           row.Rationale,
		ProposedSQLURI:      row.ProposedSQLURI,
		DiffURI:             row.DiffURI,
		CandidateFixSQLURI:  row.CandidateFixSQLURI,
		CandidateFixDiffURI: row.CandidateFixDiffURI,
		SourceResolved:      row.SourceResolved,
		Repo:                row.Repo,
		CommitSHA:           row.CommitSHA,
		FilePath:            row.FilePath,
		Edits:               editsOrLegacy(row.FileEdits, row.FilePath, row.ProposedSQLURI, row.DiffURI),
		Model:               row.Model,
		CreatedAt:           row.CreatedAt,
		PrURL:               row.PrURL,
		PrNumber:            row.PrNumber,
		PrState:             row.PrState,
		PrOpenedAt:          row.PrOpenedAt,
		PrOpenedBy:          row.PrOpenedBy,
		PrClosedAt:          row.PrClosedAt,
	}
}

const proposalColumns = `id, source, release_id, remediation_round, node_id, error_signature, attempt,
		       status, shadow_release_id, verify_error, trigger_payload,
		       confidence, rationale, proposed_sql_uri, diff_uri,
		       candidate_fix_sql_uri, candidate_fix_diff_uri, source_resolved,
		       repo, commit_sha, file_path, file_edits, model, created_at,
		       pr_url, pr_number, pr_state, pr_opened_at, pr_opened_by, pr_closed_at,
		       resolved_node_ids, node_outcomes, verifications`

// claimRow is the persistence DTO for the BeginPR RETURNING projection.
type claimRow struct {
	ID              string    `db:"id"`
	Repo            string    `db:"repo"`
	CommitSHA       string    `db:"commit_sha"`
	FilePath        string    `db:"file_path"`
	ProposedSQLURI  string    `db:"proposed_sql_uri"`
	DiffURI         string    `db:"diff_uri"`
	FileEdits       []byte    `db:"file_edits"`
	ReleaseID       string    `db:"release_id"`
	NodeID          string    `db:"node_id"`
	ResolvedNodeIDs []byte    `db:"resolved_node_ids"`
	NodeOutcomes    []byte    `db:"node_outcomes"`
	Attempt         int       `db:"attempt"`
	Rationale       string    `db:"rationale"`
	Confidence      string    `db:"confidence"`
	Model           string    `db:"model"`
	ClaimedAt       time.Time `db:"pr_claimed_at"`
}

// toClaim projects a claimed row onto the claim the caller opens a pull request
// from, scoped to service. ResolvedNodeIDs is first narrowed to the nodes the
// attempt actually repaired (FixedNodeIDs): the claim is what names the pull
// request and writes its body, and a node the attempt skipped or failed carries
// no fix to describe. Every consumer of a claim reads it through this
// projection, so the narrowing cannot be bypassed.
//
// When service is non-empty the claim is narrowed a second time to that owning
// service's group: Edits keeps only the files service owns (GroupEditsByService
// on serviceRepoPaths), and ResolvedNodeIDs keeps only the members those edits
// address that the attempt also fixed (MembersOfEdits ∩ fixed). The legacy ""
// service keeps every edit and the full fixed set — one pull request for the
// whole proposal.
func (row claimRow) toClaim(service, branch string, serviceRepoPaths map[string]string) proposal.PRClaim {
	edits := editsOrLegacy(row.FileEdits, row.FilePath, row.ProposedSQLURI, row.DiffURI)
	fixed := proposal.FixedNodeIDs(
		resolvedOrLegacy(row.ResolvedNodeIDs, row.NodeID), unmarshalNodeOutcomes(row.NodeOutcomes))
	resolved := fixed
	if service != "" {
		edits = proposal.GroupEditsByService(serviceRepoPaths, edits)[service]
		// The fallback is nil, not fixed: a per-service claim resolves ONLY the
		// members its own edits explicitly attribute. An edit written before the
		// member_node_ids codec carries no members, and falling back to the whole
		// fixed set there would let one service's PR claim nodes it never touched.
		resolved = proposal.IntersectSorted(proposal.MembersOfEdits(edits, nil), fixed)
	}
	return proposal.PRClaim{
		ID:              row.ID,
		Repo:            row.Repo,
		CommitSHA:       row.CommitSHA,
		FilePath:        row.FilePath,
		ProposedSQLURI:  row.ProposedSQLURI,
		DiffURI:         row.DiffURI,
		Edits:           edits,
		ReleaseID:       row.ReleaseID,
		NodeID:          row.NodeID,
		ResolvedNodeIDs: resolved,
		Attempt:         row.Attempt,
		Rationale:       row.Rationale,
		Confidence:      proposal.Confidence(row.Confidence),
		Model:           row.Model,
		ClaimedAt:       row.ClaimedAt,
		Branch:          branch,
		Service:         service,
	}
}

// pullRequestRow is the persistence DTO for one proposal_pull_request child
// row — one pull request per (proposal, service).
type pullRequestRow struct {
	ProposalID  string     `db:"proposal_id"`
	Service     string     `db:"service"`
	Repo        string     `db:"repo"`
	Branch      string     `db:"branch"`
	PrState     string     `db:"pr_state"`
	PrURL       string     `db:"pr_url"`
	PrNumber    int        `db:"pr_number"`
	PrClaimedAt *time.Time `db:"pr_claimed_at"`
	PrOpenedAt  *time.Time `db:"pr_opened_at"`
	PrOpenedBy  string     `db:"pr_opened_by"`
	PrClosedAt  *time.Time `db:"pr_closed_at"`
}

func (row pullRequestRow) toPullRequest() proposal.PullRequest {
	return proposal.PullRequest{
		Service:     row.Service,
		Repo:        row.Repo,
		Branch:      row.Branch,
		PrURL:       row.PrURL,
		PrNumber:    row.PrNumber,
		PrState:     row.PrState,
		PrOpenedAt:  row.PrOpenedAt,
		PrClosedAt:  row.PrClosedAt,
		PrOpenedBy:  row.PrOpenedBy,
		PrClaimedAt: row.PrClaimedAt,
	}
}

// pullRequestColumns is the child-table projection loaded onto a View's
// PullRequests. proposal_id is included so a batch load over many proposals can
// group the rows by their parent.
const pullRequestColumns = `proposal_id, service, repo, branch, pr_state, pr_url, pr_number,
	pr_claimed_at, pr_opened_at, pr_opened_by, pr_closed_at`

// loadPullRequests reads the proposal_pull_request child rows for the given
// proposal ids, grouped by proposal_id and ordered by service within each
// group, so applyChildPRs can populate every View's PullRequests and derive its
// singular Pr* fields from the first child.
func (r *ProposalRepository) loadPullRequests(ctx context.Context, ids []string) (map[string][]proposal.PullRequest, error) {
	byProposal := make(map[string][]proposal.PullRequest, len(ids))
	if len(ids) == 0 {
		return byProposal, nil
	}
	q := `SELECT ` + pullRequestColumns + `
		FROM proposal_pull_request
		WHERE proposal_id = ANY($1::uuid[])
		ORDER BY proposal_id, service`
	var rows []pullRequestRow
	if err := r.q.SelectContext(ctx, &rows, q, pq.Array(ids)); err != nil {
		return nil, fmt.Errorf("load proposal pull requests: %w", err)
	}
	for _, row := range rows {
		byProposal[row.ProposalID] = append(byProposal[row.ProposalID], row.toPullRequest())
	}
	return byProposal, nil
}

// applyChildPRs attaches the child pull requests to the view and, when any
// exist, overrides the view's singular Pr* fields from the first child (ordered
// by service). This keeps every existing reader of the singular fields
// rendering the live PR state now that the parent's own pr_* columns are no
// longer written; a proposal with no child rows keeps its legacy parent values.
func applyChildPRs(v *proposal.View, prs []proposal.PullRequest) {
	v.PullRequests = prs
	if len(prs) == 0 {
		return
	}
	first := prs[0]
	v.PrState = first.PrState
	v.PrURL = first.PrURL
	v.PrNumber = first.PrNumber
	v.PrOpenedAt = first.PrOpenedAt
	v.PrOpenedBy = first.PrOpenedBy
	v.PrClosedAt = first.PrClosedAt
}

// Get returns the full View for the given proposal id.
// Returns ErrNotFound if no row exists.
func (r *ProposalRepository) Get(ctx context.Context, id string) (proposal.View, error) {
	query := `SELECT ` + proposalColumns + ` FROM proposal WHERE id = $1`
	var row proposalRow
	if err := r.q.GetContext(ctx, &row, query, id); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return proposal.View{}, repository.ErrNotFound
		}
		return proposal.View{}, fmt.Errorf("get proposal: %w", err)
	}
	view := row.toView()
	byProposal, err := r.loadPullRequests(ctx, []string{id})
	if err != nil {
		return proposal.View{}, err
	}
	applyChildPRs(&view, byProposal[id])
	return view, nil
}

// List returns proposals matching the filter, ordered by created_at DESC.
// Empty filter fields are treated as "no constraint". Limit=0 means no limit.
func (r *ProposalRepository) List(ctx context.Context, filter repository.ProposalFilter) ([]proposal.View, error) {
	q := `SELECT ` + proposalColumns + ` FROM proposal WHERE 1=1`
	args := make([]any, 0, 6)

	if filter.Status != "" {
		args = append(args, filter.Status)
		q += fmt.Sprintf(" AND status = $%d", len(args))
	}
	if filter.PRState != "" {
		args = append(args, filter.PRState)
		n := len(args)
		// pr_state now lives on the proposal_pull_request child rows; the
		// parent's own pr_state column is a legacy read-mirror that nothing
		// writes. A proposal matches when any of its child rows is in the
		// requested state, or — for a legacy proposal that never split into
		// child rows — when its own parent column is. This mirrors the derived
		// singular pr_state readers see (the first child row ordered by service,
		// else the parent column).
		q += fmt.Sprintf(` AND (EXISTS (SELECT 1 FROM proposal_pull_request ppr`+
			` WHERE ppr.proposal_id = proposal.id AND ppr.pr_state = $%d)`+
			` OR (NOT EXISTS (SELECT 1 FROM proposal_pull_request ppr`+
			` WHERE ppr.proposal_id = proposal.id) AND proposal.pr_state = $%d))`, n, n)
	}
	if filter.ReleaseID != "" {
		args = append(args, filter.ReleaseID)
		q += fmt.Sprintf(" AND release_id = $%d", len(args))
	}
	if filter.Source != "" {
		args = append(args, filter.Source)
		q += fmt.Sprintf(" AND source = $%d", len(args))
	}
	if filter.NodeID != "" {
		args = append(args, filter.NodeID)
		q += fmt.Sprintf(" AND resolved_node_ids ? $%d", len(args))
	}
	q += " ORDER BY created_at DESC"
	if filter.Limit > 0 {
		args = append(args, filter.Limit)
		q += fmt.Sprintf(" LIMIT $%d", len(args))
	}

	var rows []proposalRow
	if err := r.q.SelectContext(ctx, &rows, q, args...); err != nil {
		return nil, fmt.Errorf("list proposals: %w", err)
	}
	views := make([]proposal.View, 0, len(rows))
	ids := make([]string, 0, len(rows))
	for _, row := range rows {
		views = append(views, row.toView())
		ids = append(ids, row.ID)
	}
	byProposal, err := r.loadPullRequests(ctx, ids)
	if err != nil {
		return nil, err
	}
	for i := range views {
		applyChildPRs(&views[i], byProposal[views[i].ID])
	}
	return views, nil
}

// beginPRClaimCAS is the atomic claim BeginPR issues: it moves a (proposal,
// service) child row from '' or 'failed' to 'opening' — stamping pr_claimed_at
// — only while the parent proposal is still source-resolved and 'proposed'. The
// correlated EXISTS re-checks those two preconditions inside the same UPDATE as
// the pr_state guard, so a status or source_resolved change committed between
// BeginPR's pre-read and this statement can never yield a claim: the UPDATE
// simply matches 0 rows (surfacing as ErrPRConflict) rather than opening a PR
// for a fix that is no longer proposed or resolved. Reading pr_claimed_at back
// from the row is what makes the returned claim carry the value actually
// persisted, not the caller's argument.
const beginPRClaimCAS = `
	UPDATE proposal_pull_request ppr
	   SET pr_state='opening', pr_claimed_at=$3
	 WHERE ppr.proposal_id=$1 AND ppr.service=$2 AND ppr.pr_state IN ('', 'failed')
	   AND EXISTS (SELECT 1 FROM proposal p
	                WHERE p.id=ppr.proposal_id AND p.source_resolved AND p.status='proposed')
	RETURNING pr_claimed_at`

// BeginPR atomically claims one (proposal, service) pull request for creation:
// the child proposal_pull_request row's pr_state moves from ” or 'failed' to
// 'opening', stamping pr_claimed_at with claimedAt. The child row is created on
// first claim by an INSERT … ON CONFLICT DO NOTHING; the UPDATE … RETURNING is
// the single-winner guard, so concurrent callers for the same service see 0
// rows and receive ErrPRConflict. The RETURNING clause reads pr_claimed_at back
// from the row rather than trusting the claimedAt argument verbatim, so the
// returned PRClaim.ClaimedAt is always the value actually persisted — the one a
// later FailStuckOpeningPR call must CAS against.
//
// The parent-proposal preconditions are read first so each returns its own
// error rather than collapsing into the CAS's "already claimed". Claiming
// requires status='proposed' as well as source_resolved: a python contract fix
// is written as 'verifying' with source_resolved=true the moment its shadow
// release is submitted, and a shadow rejection changes nothing but the status —
// so without the status guard a caller could open a pull request for a fix
// still being judged, or for one already judged wrong.
//
// Returns ErrNotSourceResolved when source_resolved=false, ErrNotProposed when
// the attempt has not reached 'proposed', ErrPRConflict when the service is
// already claimed, ErrNotFound when the id is unknown.
func (r *ProposalRepository) BeginPR(ctx context.Context, id, service, branch string, claimedAt time.Time) (proposal.PRClaim, error) {
	// Pre-read the parent proposal: the claim's payload columns plus the
	// source_resolved / status ladder. pr_claimed_at is not among them — the
	// claim ages from the child row's CAS, not from the (legacy) parent column.
	var pre struct {
		claimRow
		SourceResolved bool   `db:"source_resolved"`
		Status         string `db:"status"`
	}
	const preRead = `SELECT id, repo, commit_sha, file_path, proposed_sql_uri, diff_uri,
		file_edits, release_id, node_id, resolved_node_ids, node_outcomes,
		attempt, rationale, confidence, model, source_resolved, status
		FROM proposal WHERE id=$1`
	if err := r.q.GetContext(ctx, &pre, preRead, id); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return proposal.PRClaim{}, repository.ErrNotFound
		}
		return proposal.PRClaim{}, fmt.Errorf("begin pr lookup: %w", err)
	}
	if !pre.SourceResolved {
		return proposal.PRClaim{}, repository.ErrNotSourceResolved
	}
	if proposal.Status(pre.Status) != proposal.StatusProposed {
		return proposal.PRClaim{}, repository.ErrNotProposed
	}

	// Create the child row if this service has never been claimed; a row left
	// from an earlier failed claim is untouched (DO NOTHING) so its CAS below
	// re-claims it from 'failed'.
	if _, err := r.q.ExecContext(ctx,
		`INSERT INTO proposal_pull_request (proposal_id, service, repo, branch)
		 VALUES ($1,$2,$3,$4) ON CONFLICT (proposal_id, service) DO NOTHING`,
		id, service, pre.Repo, branch); err != nil {
		return proposal.PRClaim{}, fmt.Errorf("begin pr insert child: %w", err)
	}

	var persistedClaimedAt time.Time
	if err := r.q.GetContext(ctx, &persistedClaimedAt, beginPRClaimCAS,
		id, service, claimedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return proposal.PRClaim{}, repository.ErrPRConflict
		}
		return proposal.PRClaim{}, fmt.Errorf("begin pr cas: %w", err)
	}

	row := pre.claimRow
	row.ClaimedAt = persistedClaimedAt
	return row.toClaim(service, branch, r.serviceRepoPaths), nil
}

// RecordPR records the opened PR on the (proposal, service) child row, flips
// its pr_state to 'open', and clears pr_claimed_at back to NULL since the claim
// it tracked is now resolved — but only when the row is still 'opening': the
// WHERE clause is the same compare-and-set guard FailStuckOpeningPR and
// RecordPROutcome apply, so a row already moved on (recorded or failed by
// another caller, including an unknown id) leaves 0 rows affected rather than a
// blind write. hit reports whether the CAS fired.
func (r *ProposalRepository) RecordPR(ctx context.Context, id, service, prURL string, prNumber int, openedBy string, openedAt time.Time) (bool, error) {
	res, err := r.q.ExecContext(ctx,
		`UPDATE proposal_pull_request
		 SET pr_state='open', pr_url=$3, pr_number=$4, pr_opened_by=$5, pr_opened_at=$6,
		     pr_claimed_at=NULL
		 WHERE proposal_id=$1 AND service=$2 AND pr_state='opening'`,
		id, service, prURL, prNumber, openedBy, openedAt)
	if err != nil {
		return false, fmt.Errorf("record pr: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("record pr: rows affected: %w", err)
	}
	return n > 0, nil
}

// FailStuckOpeningPR resets a stuck 'opening' claim back to 'failed' and
// clears pr_claimed_at, but only when the row's current pr_claimed_at still
// equals observedClaimedAt: the WHERE clause is the compare-and-set guard, so
// a claim released and re-claimed since the caller observed or acquired it (a
// different pr_claimed_at, or a pr_state that already moved on) leaves 0 rows
// affected rather than clobbering the fresh claim. Used both by the
// reconciler's opening sweep (CAS'd on the ClaimedAt it read while listing
// stuck claims) and by the ui PR-creation route's own failure
// callback (CAS'd on the ClaimedAt its own BeginPR call returned).
func (r *ProposalRepository) FailStuckOpeningPR(ctx context.Context, id, service string, observedClaimedAt time.Time) (bool, error) {
	res, err := r.q.ExecContext(ctx,
		`UPDATE proposal_pull_request SET pr_state='failed', pr_claimed_at=NULL
		 WHERE proposal_id=$1 AND service=$2 AND pr_state='opening' AND pr_claimed_at=$3`,
		id, service, observedClaimedAt)
	if err != nil {
		return false, fmt.Errorf("fail stuck opening pr: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("fail stuck opening pr: rows affected: %w", err)
	}
	return n > 0, nil
}

// openPRRow is the persistence DTO for the ListOpenPullRequests projection: one
// row per open child pull request, joined to its parent proposal.
type openPRRow struct {
	ID        string `db:"id"`
	Service   string `db:"service"`
	Repo      string `db:"repo"`
	PRNumber  int    `db:"pr_number"`
	ReleaseID string `db:"release_id"`
	NodeID    string `db:"node_id"`
	Attempt   int    `db:"attempt"`
}

// ListOpenPullRequests returns one entry per child pull request with
// pr_state='open', oldest-opened first, so the reconciler checks the
// longest-waiting PRs before newer ones. A proposal split into several
// per-service PRs yields one entry per open service.
func (r *ProposalRepository) ListOpenPullRequests(ctx context.Context, limit int) ([]proposal.OpenPR, error) {
	q := `SELECT p.id, ppr.service, ppr.repo, ppr.pr_number, p.release_id, p.node_id, p.attempt
	      FROM proposal_pull_request ppr
	      JOIN proposal p ON p.id = ppr.proposal_id
	      WHERE ppr.pr_state = 'open' ORDER BY ppr.pr_opened_at ASC`
	args := []any{}
	if limit > 0 {
		args = append(args, limit)
		q += " LIMIT $1"
	}
	var rows []openPRRow
	if err := r.q.SelectContext(ctx, &rows, q, args...); err != nil {
		return nil, fmt.Errorf("list open pull requests: %w", err)
	}
	out := make([]proposal.OpenPR, 0, len(rows))
	for _, row := range rows {
		out = append(out, proposal.OpenPR{
			ID:        row.ID,
			Repo:      row.Repo,
			PRNumber:  row.PRNumber,
			ReleaseID: row.ReleaseID,
			NodeID:    row.NodeID,
			Attempt:   row.Attempt,
			Service:   row.Service,
		})
	}
	return out, nil
}

// openingRow is the persistence DTO for the ListStuckOpening projection: one
// row per 'opening' child pull request, joined to its parent proposal for the
// ordering key and the fields the opening sweep needs.
type openingRow struct {
	ID        string     `db:"id"`
	Service   string     `db:"service"`
	Repo      string     `db:"repo"`
	ReleaseID string     `db:"release_id"`
	NodeID    string     `db:"node_id"`
	Attempt   int        `db:"attempt"`
	ClaimedAt *time.Time `db:"pr_claimed_at"`
	CreatedAt time.Time  `db:"created_at"`
}

// ListStuckOpening returns up to limit child pull requests with
// pr_state='opening', ordered oldest-created first by the parent proposal's
// (created_at, id) and resuming strictly after cursor (nil for the first page).
// It over-fetches by one row to tell "more rows exist" apart from "this was the
// last page": when limit+1 rows come back, the (limit+1)th is dropped from the
// result and becomes next, the cursor of the row now at the end of the page;
// otherwise next is nil. The keyset is the parent proposal's (created_at, id):
// each proposal claimed today has a single 'opening' child, so the two agree
// one-to-one.
func (r *ProposalRepository) ListStuckOpening(ctx context.Context, limit int, cursor *repository.OpeningCursor) ([]proposal.OpeningPR, *repository.OpeningCursor, error) {
	if limit <= 0 {
		limit = 50
	}
	q := `SELECT p.id, ppr.service, ppr.repo, p.release_id, p.node_id, p.attempt,
	             ppr.pr_claimed_at, p.created_at
	      FROM proposal_pull_request ppr
	      JOIN proposal p ON p.id = ppr.proposal_id
	      WHERE ppr.pr_state = 'opening'`
	args := make([]any, 0, 6)
	if cursor != nil {
		args = append(args, cursor.CreatedAt, cursor.ID)
		q += fmt.Sprintf(" AND (p.created_at, p.id) > ($%d, $%d)", len(args)-1, len(args))
	}
	q += " ORDER BY p.created_at ASC, p.id ASC"
	args = append(args, limit+1)
	q += fmt.Sprintf(" LIMIT $%d", len(args))

	var rows []openingRow
	if err := r.q.SelectContext(ctx, &rows, q, args...); err != nil {
		return nil, nil, fmt.Errorf("list stuck opening proposals: %w", err)
	}

	var next *repository.OpeningCursor
	if len(rows) > limit {
		last := rows[limit-1]
		next = &repository.OpeningCursor{CreatedAt: last.CreatedAt, ID: last.ID}
		rows = rows[:limit]
	}

	out := make([]proposal.OpeningPR, 0, len(rows))
	for _, row := range rows {
		out = append(out, proposal.OpeningPR{
			ID:        row.ID,
			Repo:      row.Repo,
			ReleaseID: row.ReleaseID,
			NodeID:    row.NodeID,
			Attempt:   row.Attempt,
			ClaimedAt: row.ClaimedAt,
			CreatedAt: row.CreatedAt,
			Service:   row.Service,
		})
	}
	return out, next, nil
}

// RecordPROutcome atomically transitions the (proposal, service) child row's
// pr_state 'open' -> outcome. The WHERE pr_state='open' guard makes concurrent
// or repeated calls single-winner: only the first caller sees rows-affected=1;
// every later call is a no-op false.
func (r *ProposalRepository) RecordPROutcome(ctx context.Context, id, service string, outcome proposal.PROutcome, closedAt time.Time) (bool, error) {
	res, err := r.q.ExecContext(ctx,
		`UPDATE proposal_pull_request SET pr_state=$3, pr_closed_at=$4
		 WHERE proposal_id=$1 AND service=$2 AND pr_state='open'`,
		id, service, string(outcome), closedAt)
	if err != nil {
		return false, fmt.Errorf("record pr outcome: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("record pr outcome: rows affected: %w", err)
	}
	return n > 0, nil
}

// verifyingBatchLimit caps ListVerifying so a single stuck row can never
// starve every other in-flight verification: the polling reconciler always
// makes bounded progress on each tick.
const verifyingBatchLimit = 20

// ListVerifying returns proposals awaiting shadow-release verification
// (status='verifying'), oldest first, capped at verifyingBatchLimit.
func (r *ProposalRepository) ListVerifying(ctx context.Context) ([]proposal.View, error) {
	q := `SELECT ` + proposalColumns + ` FROM proposal
	      WHERE status = $1 ORDER BY created_at ASC LIMIT $2`
	var rows []proposalRow
	if err := r.q.SelectContext(ctx, &rows, q, proposal.StatusVerifying, verifyingBatchLimit); err != nil {
		return nil, fmt.Errorf("list verifying proposals: %w", err)
	}
	views := make([]proposal.View, 0, len(rows))
	for _, row := range rows {
		views = append(views, row.toView())
	}
	return views, nil
}

// rewriteVerifyingOutcomes is the node_outcomes assignment that carries an
// attempt's per-node outcomes along with the attempt itself: every node still
// waiting on a shadow release takes the status the attempt just reached, while
// a node the attempt had already settled — skipped, or failed while being
// fixed — keeps the entry it was recorded with, since no release judged it.
//
// The COALESCE is what keeps the column (NOT NULL) writable: aggregating over a
// row that recorded no per-node outcome at all yields SQL NULL, not an empty
// object.
const rewriteVerifyingOutcomes = `
	node_outcomes = COALESCE((
		SELECT jsonb_object_agg(k, CASE WHEN v->>'status' = $2
			THEN jsonb_set(v, '{status}', to_jsonb($3::text)) ELSE v END)
		FROM jsonb_each(node_outcomes) AS e(k, v)
	), '{}'::jsonb)`

// MarkVerified finalizes a proposal whose shadow releases validated the fix,
// transitioning status 'verifying' -> 'proposed' and carrying every node that
// was waiting on those releases to 'proposed' with it. The WHERE
// status='verifying' guard is the same compare-and-set contract as every other
// state transition in this file: a row already finalized by a concurrent or
// repeated reconciler pass leaves 0 rows affected rather than a blind
// overwrite. hit reports whether the CAS fired.
func (r *ProposalRepository) MarkVerified(ctx context.Context, id string) (bool, error) {
	res, err := r.q.ExecContext(ctx,
		`UPDATE proposal SET status=$3, `+rewriteVerifyingOutcomes+` WHERE id=$1 AND status=$2`,
		id, proposal.StatusVerifying, proposal.StatusProposed)
	if err != nil {
		return false, fmt.Errorf("mark verified: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("mark verified: rows affected: %w", err)
	}
	return n > 0, nil
}

// MarkVerifyFailed finalizes a proposal whose shadow releases failed to
// validate the fix, transitioning status 'verifying' -> 'failed', carrying
// every node that was waiting on those releases to 'failed' with it, and
// recording verifyErr so the next attempt can use it as evidence. The CAS
// guard and hit semantics mirror MarkVerified.
func (r *ProposalRepository) MarkVerifyFailed(ctx context.Context, id, verifyErr string) (bool, error) {
	res, err := r.q.ExecContext(ctx,
		`UPDATE proposal SET status=$3, verify_error=$4, `+rewriteVerifyingOutcomes+` WHERE id=$1 AND status=$2`,
		id, proposal.StatusVerifying, proposal.StatusFailed, verifyErr)
	if err != nil {
		return false, fmt.Errorf("mark verify failed: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("mark verify failed: rows affected: %w", err)
	}
	return n > 0, nil
}
