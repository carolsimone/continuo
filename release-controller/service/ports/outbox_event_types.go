package ports

// ReleasePromotedEventType is the outbox event_type of a release.promoted:v1
// row. The row's payload is the typed event; the outbox relay renders it into
// the envelope's stream fields when it publishes the row.
const ReleasePromotedEventType = "release_promoted"
