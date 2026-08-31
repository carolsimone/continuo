-- One pull request per (proposal, service). A proposal's edits are grouped by
-- the service owning each file; each group gets its own PR row with an
-- independent claim/open/close lifecycle. service = '' is the legacy sentinel:
-- one PR covering the whole proposal, created by the backfill below for rows
-- that entered the PR lifecycle before the split. The parent proposal's
-- singular pr_* columns stop being written but remain for legacy reads.
CREATE TABLE proposal_pull_request (
    proposal_id   UUID        NOT NULL REFERENCES proposal(id) ON DELETE CASCADE,
    service       TEXT        NOT NULL DEFAULT '',
    repo          TEXT        NOT NULL DEFAULT '',
    branch        TEXT        NOT NULL DEFAULT '',
    -- pr_state machine per row: '' -> 'opening' -> 'open' -> 'merged'|'rejected';
    -- 'opening' -> 'failed' is the retryable error path (same machine as the
    -- parent's old column).
    pr_state      TEXT        NOT NULL DEFAULT '',
    pr_url        TEXT        NOT NULL DEFAULT '',
    pr_number     INT         NOT NULL DEFAULT 0,
    pr_claimed_at TIMESTAMPTZ NULL,
    pr_opened_at  TIMESTAMPTZ NULL,
    pr_opened_by  TEXT        NOT NULL DEFAULT '',
    pr_closed_at  TIMESTAMPTZ NULL,
    PRIMARY KEY (proposal_id, service)
);

CREATE INDEX idx_ppr_live ON proposal_pull_request (pr_state)
    WHERE pr_state IN ('opening', 'open');

-- Backfill: every proposal that ever entered the PR lifecycle gets one legacy
-- child row (service = '') carrying its singular columns verbatim. The branch
-- reproduces BuildBranch(release_id, attempt) for the legacy shape.
INSERT INTO proposal_pull_request
    (proposal_id, service, repo, branch, pr_state, pr_url, pr_number,
     pr_claimed_at, pr_opened_at, pr_opened_by, pr_closed_at)
SELECT id, '', repo,
       'remediation/' || release_id || '/attempt' || attempt,
       pr_state, pr_url, pr_number,
       pr_claimed_at, pr_opened_at, pr_opened_by, pr_closed_at
  FROM proposal
 WHERE pr_state <> '';
