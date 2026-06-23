module github.com/carolsimone/continuo/remediation-agent

go 1.25.1

require (
	github.com/aws/aws-sdk-go-v2 v1.41.5
	github.com/aws/aws-sdk-go-v2/config v1.32.14
	github.com/aws/aws-sdk-go-v2/credentials v1.19.14
	github.com/aws/aws-sdk-go-v2/service/s3 v1.99.0
	github.com/carolsimone/continuo/orchestrator v0.0.0-00010101000000-000000000000
	github.com/carolsimone/continuo/pkg v0.0.0-00010101000000-000000000000
	github.com/google/uuid v1.6.0
	github.com/jmoiron/sqlx v1.4.0
	github.com/lib/pq v1.12.3
	github.com/redis/go-redis/v9 v9.17.2
	github.com/stretchr/testify v1.11.1
)

replace (
	github.com/carolsimone/continuo/orchestrator => ../orchestrator
	github.com/carolsimone/continuo/pkg => ../pkg
)
