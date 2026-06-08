-- Per-service production pointer: one row per dbt service, recording the
-- manifest + image currently live for that service. The full topology is
-- reconstructed by assembling every service's pointer at release time.
CREATE TABLE service_prod (
    service_name    TEXT        PRIMARY KEY,
    release_id      TEXT        NOT NULL,
    manifest_s3_key TEXT        NOT NULL,
    image_tag       TEXT        NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
