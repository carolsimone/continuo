package identity_test

import (
	"context"
	"testing"

	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/identity"
	"google.golang.org/grpc/metadata"
)

func TestOrSystem(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"real user", "okta|alice", "okta|alice"},
		{"trims whitespace", "  okta|bob  ", "okta|bob"},
		{"empty becomes system", "", "system"},
		{"whitespace-only becomes system", "   ", "system"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := identity.OrSystem(tc.in); got != tc.want {
				t.Fatalf("OrSystem(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestFromUserID_TrimsAndSentinels(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"plain", "google.com|123", "google.com|123"},
		{"trims surrounding space", "  google.com|123  ", "google.com|123"},
		{"empty becomes system", "", identity.SystemUserID},
		{"whitespace becomes system", "   ", identity.SystemUserID},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := identity.FromUserID(tc.in)
			if got.UserID != tc.want {
				t.Fatalf("FromUserID(%q).UserID = %q, want %q", tc.in, got.UserID, tc.want)
			}
		})
	}
}

func TestSystem_IsSystem(t *testing.T) {
	if !identity.System().IsSystem() {
		t.Fatal("System().IsSystem() = false, want true")
	}
	if identity.FromUserID("alice").IsSystem() {
		t.Fatal("real user reported IsSystem() = true")
	}
}

func TestFromIncomingContext_NoMetadata_System(t *testing.T) {
	id := identity.FromIncomingContext(context.Background())
	if !id.IsSystem() {
		t.Fatalf("expected system identity with no metadata, got %q", id.UserID)
	}
}

func TestFromIncomingContext_EmptyHeader_System(t *testing.T) {
	md := metadata.New(map[string]string{identity.MetadataKey: ""})
	ctx := metadata.NewIncomingContext(context.Background(), md)
	id := identity.FromIncomingContext(ctx)
	if !id.IsSystem() {
		t.Fatalf("expected system identity for empty header, got %q", id.UserID)
	}
}

func TestFromIncomingContext_PresentHeader(t *testing.T) {
	md := metadata.New(map[string]string{identity.MetadataKey: "okta.example.com|u-42"})
	ctx := metadata.NewIncomingContext(context.Background(), md)
	id := identity.FromIncomingContext(ctx)
	if id.UserID != "okta.example.com|u-42" {
		t.Fatalf("UserID = %q, want %q", id.UserID, "okta.example.com|u-42")
	}
}

func TestFromContext_FallsBackToSystem(t *testing.T) {
	id := identity.FromContext(context.Background())
	if !id.IsSystem() {
		t.Fatalf("FromContext with no value = %q, want system", id.UserID)
	}
}

func TestUnaryServerInterceptor_PlacesIdentityOnContext(t *testing.T) {
	md := metadata.New(map[string]string{identity.MetadataKey: "entra|abc"})
	ctx := metadata.NewIncomingContext(context.Background(), md)

	var seen identity.Identity
	interceptor := identity.UnaryServerInterceptor()
	_, err := interceptor(ctx, nil, nil, func(innerCtx context.Context, _ interface{}) (interface{}, error) {
		seen = identity.FromContext(innerCtx)
		return nil, nil
	})
	if err != nil {
		t.Fatalf("interceptor returned error: %v", err)
	}
	if seen.UserID != "entra|abc" {
		t.Fatalf("handler saw UserID %q, want entra|abc", seen.UserID)
	}
}

func TestUnaryServerInterceptor_NoMetadata_SystemOnContext(t *testing.T) {
	var seen identity.Identity
	interceptor := identity.UnaryServerInterceptor()
	_, err := interceptor(context.Background(), nil, nil, func(innerCtx context.Context, _ interface{}) (interface{}, error) {
		seen = identity.FromContext(innerCtx)
		return nil, nil
	})
	if err != nil {
		t.Fatalf("interceptor returned error: %v", err)
	}
	if !seen.IsSystem() {
		t.Fatalf("handler saw UserID %q, want system", seen.UserID)
	}
}

// Every request a gRPC server serves acts for the default tenant: the
// interceptor stores it beside the identity, whatever the metadata carries,
// a tenant a caller names included.
func TestUnaryServerInterceptor_StoresTheDefaultTenant(t *testing.T) {
	for name, ctx := range map[string]context.Context{
		"no metadata": context.Background(),
		"a user": metadata.NewIncomingContext(context.Background(),
			metadata.New(map[string]string{identity.MetadataKey: "okta|alice"})),
		"a tenant named in the metadata": metadata.NewIncomingContext(context.Background(),
			metadata.New(map[string]string{"tenant_id": "acme", "x-continuo-tenant-id": "acme"})),
	} {
		t.Run(name, func(t *testing.T) {
			var seen string
			_, err := identity.UnaryServerInterceptor()(ctx, nil, nil, func(innerCtx context.Context, _ interface{}) (interface{}, error) {
				seen = identity.TenantFromContext(innerCtx)
				return nil, nil
			})
			if err != nil {
				t.Fatalf("interceptor returned error: %v", err)
			}
			if seen != pkgevents.DefaultTenantID {
				t.Fatalf("handler saw tenant %q, want %q", seen, pkgevents.DefaultTenantID)
			}
		})
	}
}

// A context no interceptor placed a tenant on carries none: there is no
// fallback tenant, so a caller can refuse the request instead of guessing.
func TestTenantFromContext_NoTenantIsEmpty(t *testing.T) {
	if got := identity.TenantFromContext(context.Background()); got != "" {
		t.Fatalf("TenantFromContext with no value = %q, want empty", got)
	}
}

// WithTenant places the tenant a request acts for on its context.
func TestWithTenant_RoundTrips(t *testing.T) {
	ctx := identity.WithTenant(context.Background(), "acme")
	if got := identity.TenantFromContext(ctx); got != "acme" {
		t.Fatalf("TenantFromContext = %q, want acme", got)
	}
	if !identity.FromContext(ctx).IsSystem() {
		t.Fatal("placing a tenant must not place an identity")
	}
}
