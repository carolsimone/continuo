module github.com/carolsimone/continuo/orchestrator

go 1.25.13

replace (
	github.com/carolsimone/continuo/pkg => ../pkg
	github.com/carolsimone/continuo/state => ../state
)

require (
	github.com/DATA-DOG/go-sqlmock v1.5.2
	github.com/aws/aws-sdk-go-v2 v1.46.0
	github.com/aws/aws-sdk-go-v2/credentials v1.20.3
	github.com/aws/aws-sdk-go-v2/service/s3 v1.111.0
	github.com/carolsimone/continuo/pkg v0.0.0-00010101000000-000000000000
	github.com/carolsimone/continuo/state v0.0.0-00010101000000-000000000000
	github.com/google/uuid v1.6.0
	github.com/jmoiron/sqlx v1.4.0
	github.com/lib/pq v1.12.3
	github.com/neo4j/neo4j-go-driver/v5 v5.28.4
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2
	github.com/redis/go-redis/v9 v9.22.0
	github.com/stretchr/testify v1.12.1
	google.golang.org/grpc v1.83.2
	google.golang.org/protobuf v1.36.12-0.20260120151049-f2248ac996af
)

require (
	github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream v1.7.20 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.5.2 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.8.2 // indirect
	github.com/aws/aws-sdk-go-v2/internal/v4a v1.5.2 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.13.19 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/checksum v1.11.2 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.14.2 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/s3shared v1.20.2 // indirect
	github.com/aws/smithy-go v1.28.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	go.uber.org/atomic v1.11.0 // indirect
	go.yaml.in/yaml/v3 v3.0.5 // indirect
	golang.org/x/net v0.58.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
	golang.org/x/text v0.41.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260526163538-3dc84a4a5aaa // indirect
)
