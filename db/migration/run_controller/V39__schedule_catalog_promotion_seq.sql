-- schedule_catalog_state records the promotion seq of the last
-- schedules.loaded:v1 payload the schedule catalog applied. A payload carrying
-- a lower seq describes an older release: state acknowledges it without
-- touching the catalog, so a late or redriven older event never rolls the
-- catalog back. One row; the reconcile reads and writes it under the catalog's
-- advisory lock.

CREATE TABLE IF NOT EXISTS schedule_catalog_state (
    id            BOOLEAN     PRIMARY KEY DEFAULT TRUE,
    promotion_seq BIGINT      NOT NULL DEFAULT 0,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT schedule_catalog_state_singleton CHECK (id = TRUE)
);

INSERT INTO schedule_catalog_state (id) VALUES (TRUE) ON CONFLICT DO NOTHING;
