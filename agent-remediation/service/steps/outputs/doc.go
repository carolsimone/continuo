// Package outputs holds the steps that end a remediation. pull_request hands a
// verified proposal over for human review by announcing it on
// remediation.proposed:v1, and hand_over records that the tries ran out with no
// proposal.
package outputs
