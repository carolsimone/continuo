package neo4jinfra

import (
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

// safeString returns the string value of v, or "" if v is nil or not a string.
func safeString(v interface{}) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

// recordValue retrieves a value from a Neo4j record by key.
func recordValue(record *neo4j.Record, key string) interface{} {
	value, _ := record.Get(key)
	return value
}
