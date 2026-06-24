package sanitizer

import "github.com/carolsimone/continuo/remediation-agent/service/ports"

// Passthrough returns the dbt log unchanged.
type Passthrough struct{}

// Sanitize returns the log string as-is.
func (Passthrough) Sanitize(log string) string { return log }

var _ ports.LogSanitizer = Passthrough{}
