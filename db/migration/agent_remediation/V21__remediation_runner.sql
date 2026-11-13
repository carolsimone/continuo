-- The runner's state: one remediation per (tenant, release, round), the tries it
-- makes, the clusters of failing nodes in each try, and the steps that work on a
-- cluster. A worker claims a remediation with a lease. A remediation's failures
-- and last_error count the drives that failed before a step's row could record
-- the error: the remediation could not be loaded, or its next try could not
-- open.
CREATE TABLE remediation (
    id                 UUID PRIMARY KEY,
    tenant_id          TEXT        NOT NULL,
    release_id         TEXT        NOT NULL,
    remediation_round  INT         NOT NULL CHECK (remediation_round >= 1),
    flow               TEXT        NOT NULL,
    status             TEXT        NOT NULL CHECK (status IN ('running','done','system_failure')),
    trigger_payload    JSONB       NOT NULL,
    trigger_message_id TEXT        NOT NULL,
    trigger_fields     JSONB       NOT NULL,
    current_try        INT         NOT NULL DEFAULT 0,
    lease_owner        TEXT,
    lease_until        TIMESTAMPTZ,
    failure            TEXT        NOT NULL DEFAULT '',
    failures           INT         NOT NULL DEFAULT 0,
    last_error         TEXT        NOT NULL DEFAULT '',
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, release_id, remediation_round)
);
CREATE INDEX remediation_claimable ON remediation (created_at) WHERE status = 'running';

CREATE TABLE remediation_try (
    remediation_id UUID        NOT NULL REFERENCES remediation(id) ON DELETE CASCADE,
    try            INT         NOT NULL CHECK (try >= 1),
    attempt        INT         NOT NULL,
    status         TEXT        NOT NULL CHECK (status IN ('drafting','checking','verifying','passed','failed')),
    file_revisions JSONB       NOT NULL,
    deadline_at    TIMESTAMPTZ NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (remediation_id, try)
);

CREATE TABLE remediation_cluster (
    remediation_id UUID    NOT NULL,
    try            INT     NOT NULL,
    cluster        INT     NOT NULL CHECK (cluster >= 1),
    kind           TEXT    NOT NULL CHECK (kind IN ('single','shared_upstream','cross_service')),
    target_node_id TEXT    NOT NULL,
    members        TEXT[]  NOT NULL,
    type_name      TEXT    NOT NULL DEFAULT '',
    status         TEXT    NOT NULL CHECK (status IN ('gathering','proposing','preflight','checked','handed_over','fell_back')),
    round          INT     NOT NULL DEFAULT 1,
    outcome        JSONB,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (remediation_id, try, cluster),
    FOREIGN KEY (remediation_id, try) REFERENCES remediation_try ON DELETE CASCADE
);

CREATE TABLE remediation_step (
    id             BIGSERIAL PRIMARY KEY,
    remediation_id UUID        NOT NULL,
    try            INT         NOT NULL,
    cluster        INT         NOT NULL DEFAULT 0,   -- 0: a flow-level step
    step           TEXT        NOT NULL,
    kind           TEXT        NOT NULL CHECK (kind IN ('context','prompt','check','approver','output','runner')),
    round          INT         NOT NULL DEFAULT 1,
    status         TEXT        NOT NULL CHECK (status IN ('running','waiting','done','failed')),
    input_digest   TEXT        NOT NULL DEFAULT '',
    output         JSONB,
    findings       JSONB       NOT NULL DEFAULT '[]'::jsonb,
    waiting_on     JSONB,
    model          TEXT        NOT NULL DEFAULT '',
    prompt_version TEXT        NOT NULL DEFAULT '',
    tokens_in      INT         NOT NULL DEFAULT 0,
    tokens_out     INT         NOT NULL DEFAULT 0,
    cache_hit      BOOLEAN     NOT NULL DEFAULT false,
    file_revision  TEXT        NOT NULL DEFAULT '',
    failures       INT         NOT NULL DEFAULT 0,
    last_error     TEXT        NOT NULL DEFAULT '',
    started_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at    TIMESTAMPTZ,
    UNIQUE (remediation_id, try, cluster, step, round),
    FOREIGN KEY (remediation_id, try) REFERENCES remediation_try ON DELETE CASCADE
);

ALTER TABLE proposal ADD COLUMN remediation_id UUID NULL REFERENCES remediation(id) ON DELETE SET NULL;
CREATE INDEX proposal_remediation_id ON proposal (remediation_id) WHERE remediation_id IS NOT NULL;
