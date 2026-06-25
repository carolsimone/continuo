package identity_test

import (
	"context"
	"testing"

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

// The outgoing helper and the incoming extractor must agree on the key so a
// user_id set by a Go client round-trips back to the same Identity on the
// server. The wire moves it from outgoing to incoming metadata; this asserts
// the two helpers share MetadataKey.
func TestOutgoing_RoundTrips_ToIncoming(t *testing.T) {
	ctx := identity.NewOutgoingContext(context.Background(), identity.FromUserID("alice"))
	md, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		t.Fatal("no outgoing metadata set")
	}
	incoming := metadata.NewIncomingContext(context.Background(), md)
	id := identity.FromIncomingContext(incoming)
	if id.UserID != "alice" {
		t.Fatalf("round-tripped UserID = %q, want alice", id.UserID)
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
