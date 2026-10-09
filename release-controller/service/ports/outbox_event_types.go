package ports

// ReleasePromotedV2EventType is the outbox event_type of a release.promoted:v2
// row. The row's payload is the typed event; the outbox relay renders it into
// the envelope's stream fields when it publishes the row.
const ReleasePromotedV2EventType = "release_promoted_v2"
