CREATE TABLE threads (
    id         UUID PRIMARY KEY,
    user_id    TEXT        NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX idx_threads_user_id ON threads (user_id);
CREATE INDEX idx_threads_updated_at ON threads (updated_at);

CREATE TABLE messages (
    id         UUID PRIMARY KEY,
    thread_id  UUID        NOT NULL REFERENCES threads (id) ON DELETE CASCADE,
    seq        INTEGER     NOT NULL,
    role       TEXT        NOT NULL CHECK (role IN ('user','assistant','tool_call','tool_result')),
    content    JSONB       NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    UNIQUE (thread_id, seq)
);

CREATE TABLE pending_actions (
    id         UUID PRIMARY KEY,
    thread_id  UUID        NOT NULL REFERENCES threads (id) ON DELETE CASCADE,
    tool       TEXT        NOT NULL,
    args       JSONB       NOT NULL,
    summary    TEXT        NOT NULL,
    status     TEXT        NOT NULL CHECK (status IN ('pending','approved','denied','expired')),
    created_at TIMESTAMPTZ NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX idx_pending_actions_thread_id ON pending_actions (thread_id);
