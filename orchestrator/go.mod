module github.com/carolsimone/continuo/orchestrator

go 1.25.1

replace (
	github.com/carolsimone/continuo/pkg => ../pkg
	github.com/carolsimone/continuo/state => ../state
)

require (
	github.com/google/uuid v1.6.0
	github.com/jmoiron/sqlx v1.4.0
	github.com/lib/pq v1.10.9
	github.com/neo4j/neo4j-go-driver/v5 v5.15.0
	github.com/redis/go-redis/v9 v9.17.2
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
)
