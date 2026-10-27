package handlers

import (
	"context"

	"github.com/carolsimone/continuo/pkg/identity"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// requestTenant returns the tenant the request acts for, which the identity
// interceptor at the gRPC edge placed on ctx. Every handler reads and writes
// only that tenant's rows. A context that carries no tenant is refused with
// Internal before anything is read or written: the server serving it was
// built without the interceptor, and no handler acts for a tenant it guessed.
func requestTenant(ctx context.Context) (string, error) {
	tenantID := identity.TenantFromContext(ctx)
	if tenantID == "" {
		return "", status.Error(codes.Internal, "the request carries no tenant")
	}
	return tenantID, nil
}
