package redis_test

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"net"
	"syscall"
	"testing"

	"github.com/carolsimone/continuo/pkg/events"
	redis "github.com/carolsimone/continuo/pkg/redis"
	"github.com/lib/pq"
	"github.com/stretchr/testify/assert"
)

type statusErr int

func (s statusErr) Error() string       { return fmt.Sprintf("http %d", int(s)) }
func (s statusErr) HTTPStatusCode() int { return int(s) }

type redisReply string

func (r redisReply) Error() string { return string(r) }
func (redisReply) RedisError()     {}

func TestClassify(t *testing.T) {
	refused := &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}
	cases := []struct {
		name string
		err  error
		want redis.ErrorClass
	}{
		{"permanent", fmt.Errorf("parse: %w", events.ErrPermanent), redis.ClassPermanent},
		{"permanent wins over an outage", errors.Join(refused, events.ErrPermanent), redis.ClassPermanent},
		{"connection refused", fmt.Errorf("begin uow: %w", refused), redis.ClassInfrastructure},
		{"dns failure", &net.DNSError{Err: "no such host", Name: "postgres", IsNotFound: true}, redis.ClassInfrastructure},
		{"bare errno", fmt.Errorf("write: %w", syscall.ECONNRESET), redis.ClassInfrastructure},
		{"bad conn", driver.ErrBadConn, redis.ClassInfrastructure},
		{"pg connection failure 08006", &pq.Error{Code: "08006"}, redis.ClassInfrastructure},
		{"pg admin shutdown 57P01", &pq.Error{Code: "57P01"}, redis.ClassInfrastructure},
		{"pg starting up 57P03", &pq.Error{Code: "57P03"}, redis.ClassInfrastructure},
		{"pg too many connections 53300", &pq.Error{Code: "53300"}, redis.ClassInfrastructure},
		{"query_canceled by our own deadline", &pq.Error{Code: "57014"}, redis.ClassTransient},
		{"pg unique violation", &pq.Error{Code: "23505"}, redis.ClassTransient},
		{"pg serialization failure", &pq.Error{Code: "40001"}, redis.ClassTransient},
		{"s3 503", fmt.Errorf("get object: %w", statusErr(503)), redis.ClassInfrastructure},
		{"s3 404", statusErr(404), redis.ClassTransient},
		{"redis loading", redisReply("LOADING Redis is loading the dataset in memory"), redis.ClassInfrastructure},
		{"redis wrongtype", redisReply("WRONGTYPE Operation against a key"), redis.ClassTransient},
		{"bare deadline", context.DeadlineExceeded, redis.ClassTransient},
		{"plain error", errors.New("boom"), redis.ClassTransient},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, redis.Classify(tc.err))
		})
	}
}

func TestClassify_ExtraClassifierMarksAnOutage(t *testing.T) {
	neo4jDown := errors.New("neo4j: connectivity")
	isNeo4jDown := func(err error) bool { return errors.Is(err, neo4jDown) }
	assert.Equal(t, redis.ClassInfrastructure, redis.Classify(fmt.Errorf("swap: %w", neo4jDown), isNeo4jDown))
	assert.Equal(t, redis.ClassPermanent, redis.Classify(errors.Join(neo4jDown, events.ErrPermanent), isNeo4jDown))
}

func TestErrorClass_String(t *testing.T) {
	assert.Equal(t, "transient", redis.ClassTransient.String())
	assert.Equal(t, "permanent", redis.ClassPermanent.String())
	assert.Equal(t, "infrastructure", redis.ClassInfrastructure.String())
}
