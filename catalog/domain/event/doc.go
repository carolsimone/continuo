// Package event holds wire-format CQRS event payloads consumed by the
// catalog service. Each type maps 1:1 to a Redis stream whose name uses
// past-tense verb form (e.g. remediation.pr_opened:v1, remediation.pr_closed:v1).
//
// Internal handler-input data carriers — anything not on the wire — live
// in catalog/domain/model.
package event
