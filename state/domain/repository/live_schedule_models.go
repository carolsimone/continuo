package repository

import "context"

// LiveScheduleModels counts the models of a tenant's live release by the
// schedule name each carries: a model's first dbt tag names the schedule that
// runs it, so these counts say which schedule names the live release has. A
// model is any node of the release, of any node type; one that carries no
// schedule name is counted under none. Every read names the tenant and sees
// only that tenant's live release, and a tenant with no live release has no
// models.
type LiveScheduleModels interface {
	// CountByScheduleName returns each schedule name the tenant's live release
	// carries with how many of its models carry it. A name no model carries is
	// absent, never present with 0.
	CountByScheduleName(ctx context.Context, tenantID string) (map[string]int, error)

	// CountOf returns how many models of the tenant's live release carry
	// scheduleName; 0 when none does.
	CountOf(ctx context.Context, tenantID, scheduleName string) (int, error)
}
