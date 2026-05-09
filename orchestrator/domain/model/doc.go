// Package model holds internal domain DTOs and value objects used inside
// the orchestrator service. Types here are never serialised onto Redis
// streams as units; for that, see orchestrator/domain/event for past-tense
// wire payloads.
package model
