package event

// DeadLetteredEventType is the outbox event type of a consumer.dead_letter:v1
// entry the runner writes: a remediation ended as a system failure, and the
// heal trigger it was created from is dead-lettered so an operator can redrive
// it once the cause is fixed.
const DeadLetteredEventType = "remediation_dead_lettered"
