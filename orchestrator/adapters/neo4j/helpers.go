package neo4jinfra

import (
	"github.com/carolsimone/continuo/orchestrator/domain"
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

// recordToTableNode converts a Neo4j record to a domain.TableNode.
func recordToTableNode(record *neo4j.Record) (*domain.TableNode, error) {
	tableName, _ := record.Get("table_name")
	schemaName, _ := record.Get("schema_name")
	serviceName, _ := record.Get("service_name")
	owner, _ := record.Get("owner")
	scheduleName, _ := record.Get("schedule_name")
	criticality, _ := record.Get("criticality")
	lastUpdatedAt, _ := record.Get("last_updated_at")
	createdAt, _ := record.Get("created_at")
	nodeType, _ := record.Get("node_type")
	taskID, _ := record.Get("task_id")
	manifestVersion, _ := record.Get("manifest_version")
	imageTag, _ := record.Get("image_tag")

	node := &domain.TableNode{
		TableName:       safeString(tableName),
		SchemaName:      safeString(schemaName),
		ServiceName:     safeString(serviceName),
		Owner:           safeString(owner),
		ScheduleName:    safeString(scheduleName),
		Criticality:     domain.Criticality(safeString(criticality)),
		NodeType:        safeString(nodeType),
		TaskID:          safeString(taskID),
		ManifestVersion: safeString(manifestVersion),
		ImageTag:        safeString(imageTag),
	}

	if lastUpdatedAtNeo, ok := lastUpdatedAt.(neo4j.LocalDateTime); ok {
		node.LastUpdatedAt = lastUpdatedAtNeo.Time()
	}

	if createdAtNeo, ok := createdAt.(neo4j.LocalDateTime); ok {
		node.CreatedAt = createdAtNeo.Time()
	}

	return node, nil
}
