module github.com/carolsimone/continuo/orchestrator

go 1.25.1

replace (
	github.com/carolsimone/continuo/pkg => ../pkg
	github.com/carolsimone/continuo/state => ../state
)

require github.com/google/uuid v1.6.0

require github.com/neo4j/neo4j-go-driver/v5 v5.15.0 // indirect
