-- remediation_round counts how many times remediation has been driven for a
-- rejected release: 1 for the rejection itself, +1 per "try again". Capped in
-- code at 3. rejection_payload keeps the exact release.rejected:v1 payload the
-- rejection emitted, so a later round can replay it verbatim; releases rejected
-- before this column existed carry NULL and cannot be retried.
ALTER TABLE releases
    ADD COLUMN remediation_round INT NOT NULL DEFAULT 1,
    ADD COLUMN rejection_payload JSONB NULL;
