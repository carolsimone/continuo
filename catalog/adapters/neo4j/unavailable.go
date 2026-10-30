package neo4jinfra

import (
	"errors"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

// databaseUnavailableCode is the server's answer while a database is stopped
// or still starting.
const databaseUnavailableCode = "Neo.TransientError.General.DatabaseUnavailable"

// IsUnavailable reports whether err means Neo4j could not be reached or could
// not serve: a connectivity failure, an unavailable database, or a transaction
// whose driver retries ran out on either. The stream consumers treat these as
// an outage and wait, instead of counting a failed delivery.
func IsUnavailable(err error) bool {
	var connErr *neo4j.ConnectivityError
	if errors.As(err, &connErr) {
		return true
	}
	var dbErr *neo4j.Neo4jError
	if errors.As(err, &dbErr) && dbErr.Code == databaseUnavailableCode {
		return true
	}
	var limit *neo4j.TransactionExecutionLimit
	if errors.As(err, &limit) {
		for _, inner := range limit.Errors {
			if IsUnavailable(inner) {
				return true
			}
		}
	}
	return false
}
