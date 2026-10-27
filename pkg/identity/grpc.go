package identity

import (
	"context"

	"github.com/carolsimone/continuo/pkg/events"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// ctxKey is the unexported context key under which the extracted Identity is
// stored, so only this package can write it and external code must go through
// FromContext.
type ctxKey struct{}

// tenantKey is the unexported context key under which the tenant a request
// acts for is stored; WithTenant writes it and TenantFromContext reads it.
type tenantKey struct{}

// NewOutgoingContext attaches id to ctx's outgoing gRPC metadata under
// MetadataKey. The ui Node client sets the same header directly; this
// helper is the Go-client equivalent used by any Go caller that forwards an
// initiating user (e.g. internal services acting on a user's behalf). A system
// identity is still propagated explicitly so the receiver records "system"
// rather than guessing from an absent header.
func NewOutgoingContext(ctx context.Context, id Identity) context.Context {
	return metadata.AppendToOutgoingContext(ctx, MetadataKey, id.UserID)
}

// FromIncomingContext reads the initiating-user header from ctx's incoming gRPC
// metadata and returns the corresponding Identity. A missing, empty, or
// whitespace-only header yields the system sentinel, so unauthenticated and
// platform-initiated calls record provenance without failing.
func FromIncomingContext(ctx context.Context) Identity {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return System()
	}
	values := md.Get(MetadataKey)
	if len(values) == 0 {
		return System()
	}
	return FromUserID(values[0])
}

// FromContext returns the Identity an interceptor placed on ctx. When no
// interceptor ran (e.g. a unit test that calls a handler directly), it falls
// back to the system sentinel so handlers never see a zero-value Identity.
func FromContext(ctx context.Context) Identity {
	if id, ok := ctx.Value(ctxKey{}).(Identity); ok {
		return id
	}
	return System()
}

// withIdentity stores id on ctx for FromContext to retrieve. Unexported: the
// interceptor is the only writer.
func withIdentity(ctx context.Context, id Identity) context.Context {
	return context.WithValue(ctx, ctxKey{}, id)
}

// WithTenant returns ctx carrying tenantID as the tenant the request acts
// for. UnaryServerInterceptor writes it for every request a gRPC server
// serves; a test builds the context of a request with it.
func WithTenant(ctx context.Context, tenantID string) context.Context {
	return context.WithValue(ctx, tenantKey{}, tenantID)
}

// TenantFromContext returns the tenant the request acts for, or "" when no
// interceptor placed one on ctx. Unlike FromContext it has no fallback: a
// caller that finds no tenant refuses the request rather than act for a
// tenant it guessed.
func TenantFromContext(ctx context.Context) string {
	tenantID, _ := ctx.Value(tenantKey{}).(string)
	return tenantID
}

// UnaryServerInterceptor extracts the initiating-user Identity from incoming
// gRPC metadata and places it on the request context before the handler runs,
// beside the tenant the request acts for, which is always
// events.DefaultTenantID: the edge resolves no other tenant. It is the single
// adapter-boundary seam that converts transport metadata into the Identity
// value and the tenant; downstream application code reads them via
// FromContext and TenantFromContext and never parses metadata itself.
func UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req interface{},
		_ *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (interface{}, error) {
		ctx = WithTenant(withIdentity(ctx, FromIncomingContext(ctx)), events.DefaultTenantID)
		return handler(ctx, req)
	}
}
