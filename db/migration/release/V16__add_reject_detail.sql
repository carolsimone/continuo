-- Operator-facing explanation of a rejection, mirroring the error_detail
-- carried on release.rejected:v1. Persisted so GET /releases/{id} and the
-- release detail page can say what actually failed instead of showing only the
-- reason token. Existing rows keep '' — their detail was only ever emitted on
-- the event and is not recoverable.
ALTER TABLE releases ADD COLUMN reject_detail TEXT NOT NULL DEFAULT '';
