package sanitizer

import "github.com/carolsimone/continuo/agent-remediation/service/ports"

// Passthrough is the LogSanitizer of the legacy lanes: it returns a dbt log or a
// source text exactly as it was given. The runner does not use it; the runner
// cleans every text it sends the model with its own pkg/sanitize.Redactor.
type Passthrough struct{}

// Sanitize returns log unchanged.
func (Passthrough) Sanitize(log string) string { return log }

var _ ports.LogSanitizer = Passthrough{}
