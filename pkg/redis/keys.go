package redis

import "github.com/carolsimone/continuo/pkg/streams"

// Key is the Redis key of the contract stream name: streams.KeyPrefix followed
// by the name. Every stream the system reads or writes lives under it.
func Key(name string) string { return streams.KeyPrefix + name }

// LegacyKey is the unprefixed Redis key of the contract stream name. A consumer
// reads it beside Key(name) while streams.LegacyKeys is true and the key holds
// the consumer's group, so the entries written there before the prefix are
// still delivered.
func LegacyKey(name string) string { return name }
