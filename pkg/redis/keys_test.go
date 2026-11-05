package redis_test

import (
	"testing"

	redis "github.com/carolsimone/continuo/pkg/redis"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/stretchr/testify/assert"
)

func TestKey_PrefixesTheContractName(t *testing.T) {
	assert.Equal(t, "continuo:task.dispatch:v1", redis.Key(streams.TaskDispatchV1))
	assert.Equal(t, streams.KeyPrefix+streams.ConsumerDeadLetterV1, redis.Key(streams.ConsumerDeadLetterV1))
}
