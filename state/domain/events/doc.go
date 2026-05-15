// Package events defines the typed handler-facing event structs for the
// state service. Adapter-layer parsers (state/adapters/redis/*_parser.go)
// translate raw Redis messages into these types; service handlers consume
// them and never see JSON, goredis, or *sqlx.DB.
package events
