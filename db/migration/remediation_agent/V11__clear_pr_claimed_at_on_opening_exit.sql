-- Every transition of pr_state out of 'opening' clears pr_claimed_at back to
-- NULL at the database boundary, regardless of what value (if any) the
-- transitioning UPDATE statement itself wrote to the column. A
-- proposal-service binary that predates the pr_claimed_at column issues its
-- FailPR and RecordPR statements without mentioning the column at all, so
-- without this rule such a statement would leave the exiting row's timestamp
-- in place; if that same binary later re-enters 'opening' on the same row,
-- its UPDATE does not touch the column either, so the fill-when-NULL rule
-- below never fires (the column is not NULL) and the fresh claim would
-- silently inherit the previous claim's age. Combined with fill-when-NULL, a
-- row entering 'opening' is therefore always guaranteed to find
-- pr_claimed_at NULL beforehand, so it always receives a claim time from its
-- own transition -- whether that value comes from the writer's own explicit
-- column or from clock_timestamp() here -- and never from an earlier claim.
-- This is what lets the opening sweep compute a claim's age safely.
CREATE OR REPLACE FUNCTION stamp_pr_claimed_at_on_opening() RETURNS trigger AS $$
BEGIN
    IF OLD.pr_state = 'opening' AND NEW.pr_state <> 'opening' THEN
        NEW.pr_claimed_at := NULL;
    ELSIF NEW.pr_state = 'opening' AND NEW.pr_claimed_at IS NULL THEN
        NEW.pr_claimed_at := clock_timestamp();
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Clears pr_claimed_at on any row currently outside 'opening' that still
-- carries one -- a stale value left behind by an old binary's FailPR or
-- RecordPR write before this trigger update took effect -- restoring the
-- invariant that the column is non-NULL only while pr_state = 'opening'.
UPDATE proposal SET pr_claimed_at = NULL
WHERE pr_state <> 'opening' AND pr_claimed_at IS NOT NULL;
