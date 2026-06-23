// Package failure holds shared failure-domain value types used when decoding the
// remediation.requested:v1 trigger.
package failure

type Source string

const SourceValidation Source = "validation"
