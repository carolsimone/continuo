package redis

import (
	"database/sql/driver"
	"errors"
	"io"
	"net"
	"strings"
	"syscall"

	"github.com/carolsimone/continuo/pkg/events"
	goredis "github.com/redis/go-redis/v9"
)

// ErrorClass is how a consumer treats a handler error.
type ErrorClass int

const (
	// ClassTransient errors count toward maxDeliveries; a message whose
	// handler still fails on its last delivery is dead-lettered.
	ClassTransient ErrorClass = iota
	// ClassPermanent errors (events.ErrPermanent) are dead-lettered at once.
	ClassPermanent
	// ClassInfrastructure errors mean a dependency is unreachable. They never
	// count toward maxDeliveries: the consumer retries the same message with
	// capped exponential backoff until the dependency answers.
	ClassInfrastructure
)

func (c ErrorClass) String() string {
	switch c {
	case ClassPermanent:
		return "permanent"
	case ClassInfrastructure:
		return "infrastructure"
	default:
		return "transient"
	}
}

// InfraClassifier reports whether err means a service-specific dependency
// (for orchestrator, Neo4j) is unreachable.
type InfraClassifier func(error) bool

// Classify returns the class of a handler error. A permanent error wins over
// every other signal in the chain, so a handler that gives up explicitly is
// never retried.
func Classify(err error, extra ...InfraClassifier) ErrorClass {
	if errors.Is(err, events.ErrPermanent) {
		return ClassPermanent
	}
	if isInfrastructure(err) {
		return ClassInfrastructure
	}
	for _, f := range extra {
		if f != nil && f(err) {
			return ClassInfrastructure
		}
	}
	return ClassTransient
}

// unreachableErrnos are the socket errors that mean the peer cannot be reached.
var unreachableErrnos = []syscall.Errno{
	syscall.ECONNREFUSED, syscall.ECONNRESET, syscall.ECONNABORTED, syscall.EPIPE,
	syscall.ETIMEDOUT, syscall.EHOSTUNREACH, syscall.ENETUNREACH,
}

// isInfrastructure recognises the outages every service shares: a socket
// error, a broken database connection, a Postgres error in classes 08
// (connection), 53 (insufficient resources) or 57 (operator intervention)
// other than 57014, an HTTP 5xx, a stream cut short mid-body
// (io.ErrUnexpectedEOF, a peer that closed before delivering what it promised),
// and the Redis replies a server sends while it
// cannot serve. 57014 is query_canceled, which a handler's own deadline causes.
// Only concrete net error types count: context.DeadlineExceeded also
// implements net.Error.
func isInfrastructure(err error) bool {
	var state interface{ SQLState() string }
	if errors.As(err, &state) {
		code := state.SQLState()
		if code == "57014" {
			return false
		}
		return strings.HasPrefix(code, "08") || strings.HasPrefix(code, "53") || strings.HasPrefix(code, "57")
	}
	var opErr *net.OpError
	var dnsErr *net.DNSError
	if errors.As(err, &opErr) || errors.As(err, &dnsErr) || errors.Is(err, driver.ErrBadConn) ||
		errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	for _, errno := range unreachableErrnos {
		if errors.Is(err, errno) {
			return true
		}
	}
	var status interface{ HTTPStatusCode() int }
	if errors.As(err, &status) && status.HTTPStatusCode() >= 500 {
		return true
	}
	var reply goredis.Error
	if errors.As(err, &reply) {
		for _, prefix := range transientReplyPrefixes {
			if strings.HasPrefix(reply.Error(), prefix) {
				return true
			}
		}
	}
	return false
}
