package neo4jinfra

import (
	"errors"
	"fmt"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/stretchr/testify/assert"
)

func TestIsUnavailable(t *testing.T) {
	conn := &neo4j.ConnectivityError{Inner: errors.New("dial tcp: connection refused")}
	deadlock := &neo4j.Neo4jError{Code: "Neo.TransientError.Transaction.DeadlockDetected"}
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"connectivity", fmt.Errorf("swap topology: %w", conn), true},
		{"database unavailable", &neo4j.Neo4jError{Code: "Neo.TransientError.General.DatabaseUnavailable"}, true},
		{"retries ran out on connectivity", &neo4j.TransactionExecutionLimit{Cause: "timeout", Errors: []error{conn}}, true},
		{"deadlock is not an outage", deadlock, false},
		{"retries ran out on deadlocks", &neo4j.TransactionExecutionLimit{Cause: "timeout", Errors: []error{deadlock}}, false},
		{"constraint violation", &neo4j.Neo4jError{Code: "Neo.ClientError.Schema.ConstraintValidationFailed"}, false},
		{"plain", errors.New("boom"), false},
		{"nil", nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) { assert.Equal(t, tc.want, IsUnavailable(tc.err)) })
	}
}
