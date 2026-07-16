-- Pin each service's production pointer to the runtime manifest artifact its
-- nodes execute against, so an unchanged service keeps its own artifact when a
-- later incremental release promotes a different service.
--
-- The four columns are nullable: a pointer written before runtime manifests
-- existed carries none, and its nodes keep parsing the dbt project in-Job. They
-- are set together or not at all — a partial reference names an artifact that
-- cannot be verified — which the CHECK enforces at the row level.
ALTER TABLE service_prod
    ADD COLUMN runtime_manifest_uri TEXT NULL,
    ADD COLUMN runtime_manifest_sha256 TEXT NULL,
    ADD COLUMN runtime_manifest_dbt_version TEXT NULL,
    ADD COLUMN runtime_manifest_parse_context_sha256 TEXT NULL,
    ADD CONSTRAINT service_prod_runtime_manifest_all_or_none CHECK (
        (runtime_manifest_uri IS NULL
         AND runtime_manifest_sha256 IS NULL
         AND runtime_manifest_dbt_version IS NULL
         AND runtime_manifest_parse_context_sha256 IS NULL)
        OR
        (runtime_manifest_uri IS NOT NULL
         AND runtime_manifest_sha256 IS NOT NULL
         AND runtime_manifest_dbt_version IS NOT NULL
         AND runtime_manifest_parse_context_sha256 IS NOT NULL)
    );
