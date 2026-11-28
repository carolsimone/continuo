// Package ports holds catalog application-layer (technical) ports —
// collaborators that are not domain concepts. Adapters implement these, except
// BackupFreshness, which the service layer implements and the metrics adapter
// consumes.
package ports
