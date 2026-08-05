-- pr_claimed_at records the wall-clock moment BeginPR claims a proposal for PR
-- creation (pr_state -> 'opening'), so the reconciler's opening sweep can
-- measure how long a claim has been held. It is cleared back to NULL whenever
-- pr_state leaves 'opening' (RecordPR, FailPR), so a later re-claim of the
-- same proposal ages from its own claim time rather than a stale one.
--
-- The column is nullable: every row not currently claimed (pr_state <> 'opening')
-- carries NULL, and so would a row that is still 'opening' with no claim time
-- of its own -- backfilled below to this migration's run time instead, so such
-- a claim is granted one full grace period measured from deploy before the
-- sweep can ever release it, rather than being swept the moment the sweep
-- goes live.
ALTER TABLE proposal ADD COLUMN pr_claimed_at TIMESTAMPTZ NULL;

UPDATE proposal SET pr_claimed_at = NOW() WHERE pr_state = 'opening';
