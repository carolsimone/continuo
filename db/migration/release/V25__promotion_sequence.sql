-- Promotion seqs: one strictly increasing number per announcement of a
-- topology to the rest of continuo — a promotion, or a re-announcement of a
-- topology already live. Consumers keep the highest seq they applied and
-- ignore a lower one, so a late or redelivered announcement never replaces a
-- newer topology. The single row is created here and only ever incremented.
CREATE TABLE promotion_sequence (
    id       smallint PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    last_seq bigint   NOT NULL
);
INSERT INTO promotion_sequence (id, last_seq) VALUES (1, 0);

-- current_prod references the promoted release's topology artifact in S3 and
-- records the promotion seq it was last announced under. topology_snapshot is
-- read only by the one-time write of the artifact for a release promoted
-- before artifacts existed; every write of current_prod clears it.
ALTER TABLE current_prod
    ADD COLUMN topology_uri    text,
    ADD COLUMN topology_sha256 text,
    ADD COLUMN node_count      integer,
    ADD COLUMN promotion_seq   bigint NOT NULL DEFAULT 0,
    ALTER COLUMN topology_snapshot DROP NOT NULL;
