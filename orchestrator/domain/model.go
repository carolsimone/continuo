package domain

// EventTypeReleasePromoted is the event_type of the orchestrator_outbox row the
// topology swap writes for schedules.loaded:v1. The emit site
// (service/handlers) and the publisher's Render switch (adapters/publisher)
// share this one declaration. The value is the stored event_type and must not
// change.
const EventTypeReleasePromoted = "release_promoted"
