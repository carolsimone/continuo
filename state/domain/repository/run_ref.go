package repository

import "github.com/google/uuid"

// RunRef names one run of one tenant. A scan over every tenant's runs returns
// each run as a RunRef, so the step that then acts on the run reads and
// writes it under the run's own tenant.
type RunRef struct {
	TenantID string
	RunID    uuid.UUID
}
