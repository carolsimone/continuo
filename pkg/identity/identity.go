// Package identity carries the initiating user's provenance from the system's
// HTTP edge (ui-service) into the backend gRPC services. The authenticated
// user_id travels as a single gRPC metadata header; a server interceptor reads
// it at the adapter boundary and converts it into an Identity value placed on
// the request context, which application/use-case code reads without ever
// touching raw metadata.
package identity

import "strings"

// MetadataKey is the canonical gRPC metadata header that carries the
// initiating user's stable identifier (the ui-service "issuer-host|sub").
// It is the single source of truth for both the ui-service client that sets it
// and the backend server interceptor that reads it; neither side inlines the
// string.
//
// gRPC lowercases metadata keys on the wire, so this constant is already
// lowercase to match what FromIncomingContext observes.
const MetadataKey = "x-continuo-user-id"

// SystemUserID is the provenance recorded when no authenticated user is present
// on a mutation — scheduler-triggered (cron) runs, internal event-driven
// projections, and any non-edge client that does not set MetadataKey. It is a
// sentinel, not a real account: a row stamped "system" was initiated by the
// platform itself, not by a person.
const SystemUserID = "system"

// Identity is the provenance value object threaded through the application
// layer. UserID is always populated — it is SystemUserID when no authenticated
// caller supplied one — so persistence code never has to reason about a missing
// initiator.
type Identity struct {
	// UserID is the stable initiating-user identifier, or SystemUserID for
	// platform-initiated mutations.
	UserID string
}

// System is the identity for platform-initiated mutations.
func System() Identity { return Identity{UserID: SystemUserID} }

// FromUserID builds an Identity from a raw user_id, collapsing an empty or
// whitespace-only value to the system sentinel so callers cannot accidentally
// persist a blank initiator.
func FromUserID(userID string) Identity {
	trimmed := strings.TrimSpace(userID)
	if trimmed == "" {
		return System()
	}
	return Identity{UserID: trimmed}
}

// IsSystem reports whether the identity is the platform sentinel rather than a
// real authenticated user.
func (i Identity) IsSystem() bool { return i.UserID == SystemUserID }
