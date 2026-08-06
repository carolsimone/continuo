-- Guards the opening sweep's central invariant -- an unmeasurable claim (a
-- NULL pr_claimed_at) is never failed -- against a claim taken by a
-- proposal-service binary that predates the pr_claimed_at column and so never
-- sets it. Such a binary keeps running unmodified during a rolling upgrade
-- and cannot be taught anything about the column, so the guarantee has to
-- come from the database rather than from any particular writer: a BEFORE
-- UPDATE trigger stamps pr_claimed_at with the actual wall-clock moment of
-- the transition whenever a row's pr_state is becoming 'opening' and
-- pr_claimed_at is still NULL. This applies uniformly to every writer, old or
-- new, so a claim taken during a mixed-version rollout still gets a real
-- claim time the opening sweep can safely age from, instead of staying NULL
-- (and therefore unsweepable) forever. A writer that explicitly sets
-- pr_claimed_at in the same statement (the current BeginPR) is unaffected:
-- the trigger only fills in a value the statement left NULL, it never
-- overrides one the statement provided.
CREATE OR REPLACE FUNCTION stamp_pr_claimed_at_on_opening() RETURNS trigger AS $$
BEGIN
    IF NEW.pr_state = 'opening' AND NEW.pr_claimed_at IS NULL THEN
        NEW.pr_claimed_at := clock_timestamp();
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER proposal_stamp_pr_claimed_at
    BEFORE UPDATE ON proposal
    FOR EACH ROW
    EXECUTE FUNCTION stamp_pr_claimed_at_on_opening();

-- Adopts any row already sitting in 'opening' with pr_claimed_at still NULL
-- at the moment this migration runs -- a claim an old binary took before the
-- trigger above existed. The row ages from this adoption moment, same as the
-- trigger: never from a fabricated earlier time.
UPDATE proposal SET pr_claimed_at = clock_timestamp()
WHERE pr_state = 'opening' AND pr_claimed_at IS NULL;
