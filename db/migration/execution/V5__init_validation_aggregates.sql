-- Per-(release, leg) emission sentinel. The settle path claims a row under the
-- per-release advisory lock before emitting a leg's completion event, so two
-- concurrent final nodes cannot both emit it.
CREATE TABLE validation_aggregates (
    release_id           TEXT NOT NULL,
    mode                 TEXT NOT NULL DEFAULT 'validation',
    aggregate_emitted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (release_id, mode),
    CONSTRAINT validation_aggregates_mode_check
        CHECK (mode IN ('validation','seed_build','compile'))
);
