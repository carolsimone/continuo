// Package outputs holds the steps that end a remediation. pull_request hands a
// verified fix over for human review by announcing it on
// fix.proposed:v1 and keeps the approvers of the nodes it fixes on the
// fix's row, and hand_over announces on remediation.handed_over:v1 every
// node the remediation leaves to a human, each with the reason it was handed
// over for and its owner.
package outputs
