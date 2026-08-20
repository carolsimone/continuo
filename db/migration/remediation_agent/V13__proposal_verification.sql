-- Widen the proposal status domain with an in-flight 'verifying' state, for a
-- fix whose correctness cannot be judged synchronously: after proposing a
-- fix, the python lane posts a shadow release that runs the full parse ->
-- candidate-schema -> validation pipeline, which takes minutes. The proposal
-- parks in 'verifying' carrying the shadow release's id (shadow_release_id)
-- and the original trigger payload (trigger_payload) until an asynchronous
-- reconciler polls the release and finalizes the row to 'proposed' (the fix
-- verified) or 'failed' (verify_error records the shadow's failure reason,
-- and trigger_payload lets the reconciler rebuild the trigger to retry with
-- that error as new evidence).
ALTER TABLE proposal ADD COLUMN shadow_release_id TEXT NOT NULL DEFAULT '';
ALTER TABLE proposal ADD COLUMN verify_error TEXT NOT NULL DEFAULT '';
ALTER TABLE proposal ADD COLUMN trigger_payload JSONB NOT NULL DEFAULT '{}'::jsonb;
ALTER TABLE proposal DROP CONSTRAINT proposal_status_check;
ALTER TABLE proposal ADD CONSTRAINT proposal_status_check CHECK (status IN
  ('generating','verifying','proposed','skipped','failed','escalated'));
