// Package event holds wire-format CQRS event payloads consumed/emitted by
// the orchestrator service. Each type maps 1:1 to a Redis stream whose
// name uses past-tense verb form (e.g. node.updated:v1, release.promoted:v1).
//
// Internal handler-input data carriers — anything not on the wire — live
// in orchestrator/domain/model.
package event
