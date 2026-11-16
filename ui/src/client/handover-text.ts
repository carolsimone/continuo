import type { HandoverReason } from '../server/generated/vocabulary.gen';

// HANDOVER_TEXT says why remediation handed a failing node to a human, and what
// to do next, one sentence or two per handover_reason. A reason whose fix is
// to run the release again, or that has nothing to fix in the repository, says
// so; every other reason ends with the repository as the place to fix it.
// Typing the record by HandoverReason makes the compiler require a text for
// every reason the contract declares.
export const HANDOVER_TEXT: Record<HandoverReason, string> = {
  tries_spent: 'Every try failed the checks. Fix it in the repository.',
  preflight_rounds_spent: "The agent's drafts kept failing its own checks. Fix it in the repository.",
  declined: 'The agent could not start a fix. Fix it in the repository.',
  no_type: 'continuo has no fix type for this failure yet. Fix it in the repository.',
  classifier_infra: 'An infrastructure error failed this node. Run the release again.',
  classifier_unhealable_parse: 'A fix cannot change this parse failure. Fix it in the repository.',
  no_repository: 'The release names no repository, so no fix can be opened.',
  unverifiable_edits: "The agent's edits could not be checked by a release. Fix it in the repository.",
};
