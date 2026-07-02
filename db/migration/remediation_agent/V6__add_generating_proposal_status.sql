-- Widen the proposal status domain with an in-flight 'generating' state.
--
-- A proposal row is written as 'generating' the moment the agent commits to
-- calling the model for a healable failure, and is finalized to one of the
-- terminal states ('proposed','skipped','failed','escalated') once the attempt
-- resolves. Exposing the in-flight state lets the release UI show a
-- "Generating fix…" indicator for a healable node while its fix is being
-- produced, instead of a blank cell indistinguishable from an unhealable node.
ALTER TABLE proposal DROP CONSTRAINT proposal_status_check;
ALTER TABLE proposal ADD CONSTRAINT proposal_status_check
    CHECK (status IN ('generating','proposed','skipped','failed','escalated'));
