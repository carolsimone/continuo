package redis

import (
	"errors"
	"fmt"

	goredis "github.com/redis/go-redis/v9"

	"github.com/carolsimone/continuo/pkg/streams"
)

// Key is the Redis key of the contract stream name: streams.KeyPrefix followed
// by the name. Every stream the system reads or writes lives under it.
func Key(name string) string { return streams.KeyPrefix + name }

// LegacyKey is the unprefixed Redis key of the contract stream name. A consumer
// reads it beside Key(name) while streams.LegacyKeys is true and the key holds
// the consumer's group, so the entries written there before the prefix are
// still delivered.
func LegacyKey(name string) string { return name }

// MissingStreamError maps the reply of an XADD that carried NOMKSTREAM to the
// error a producer returns. Redis answers such an XADD with a nil reply when
// the stream does not exist, which go-redis reports as redis.Nil; the producer
// never creates the stream, so it returns an error its retry loop (the outbox,
// the consumer's dead-letter write) keeps trying until the streams-init step
// has created the stream. Any other error is returned unchanged.
func MissingStreamError(key string, err error) error {
	if errors.Is(err, goredis.Nil) {
		return fmt.Errorf("stream %s does not exist; the streams-init step creates it", key)
	}
	return err
}
