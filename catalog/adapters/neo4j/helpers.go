package neo4jinfra

import (
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

// recordValue retrieves a value from a Neo4j record by key.
func recordValue(record *neo4j.Record, key string) interface{} {
	value, _ := record.Get(key)
	return value
}

// toInt64 converts a Neo4j integer value to int64; any other value is 0.
func toInt64(v interface{}) int64 {
	switch x := v.(type) {
	case int64:
		return x
	case int:
		return int64(x)
	case int32:
		return int64(x)
	case float64:
		return int64(x)
	default:
		return 0
	}
}

// intFieldPresent returns the int value of key and whether the field was
// present and non-null (a Neo4j property that does not exist reads as nil).
func intFieldPresent(rec *neo4j.Record, k string) (int, bool) {
	v, ok := rec.Get(k)
	if !ok || v == nil {
		return 0, false
	}
	return int(toInt64(v)), true
}
