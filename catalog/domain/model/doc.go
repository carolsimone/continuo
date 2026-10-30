// Package model holds internal domain DTOs and value objects used inside
// the catalog service. Types here are never serialised onto Redis
// streams as units; for that, see catalog/domain/event for past-tense
// wire payloads.
package model
