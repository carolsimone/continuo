// Package outputs holds the steps that end a remediation. pull_request hands a
// verified proposal over for human review by announcing it on
// remediation.proposed:v1, and hand_over announces on
// remediation.handed_over:v1 every node the remediation leaves to a human,
// each with the reason it was handed over for.
package outputs
