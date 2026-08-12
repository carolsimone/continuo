package sanitizer

import (
	"github.com/carolsimone/continuo/pkg/sanitize"
	"github.com/carolsimone/continuo/remediation-agent/service/ports"
)

// Passthrough adapts the shared pkg/sanitize seam to remediation-agent's
// LogSanitizer port, so dbt logs and code stored in the graph run through the
// same redaction rules.
type Passthrough struct{}

// Sanitize applies the shared redaction rules to a dbt log.
func (Passthrough) Sanitize(log string) string { return sanitize.Text(log) }

var _ ports.LogSanitizer = Passthrough{}
