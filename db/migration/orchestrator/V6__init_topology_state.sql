CREATE TABLE topology_state (
    id BOOLEAN PRIMARY KEY DEFAULT TRUE,
    topology_generation BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT topology_state_singleton CHECK (id = TRUE)
);

INSERT INTO topology_state (id) VALUES (TRUE) ON CONFLICT DO NOTHING;

COMMENT ON TABLE topology_state IS
    'Singleton row holding the monotonic topology_generation counter. Increments on every accepted manifest.loaded:v1.';
