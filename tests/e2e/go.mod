module github.com/carolsimone/continuo/e2e

go 1.25.12

replace (
	github.com/carolsimone/continuo/orchestrator => ../../orchestrator
	github.com/carolsimone/continuo/pkg => ../../pkg
	github.com/carolsimone/continuo/state => ../../state
)

require (
	github.com/aws/aws-sdk-go-v2 v1.43.2
	github.com/aws/aws-sdk-go-v2/credentials v1.19.32
	github.com/aws/aws-sdk-go-v2/service/s3 v1.106.2
	github.com/carolsimone/continuo/orchestrator v0.0.0-00010101000000-000000000000
	github.com/carolsimone/continuo/pkg v0.0.0-00010101000000-000000000000
	github.com/carolsimone/continuo/state v0.0.0-00010101000000-000000000000
	github.com/google/uuid v1.6.0
	github.com/gorilla/websocket v1.5.3
	github.com/jmoiron/sqlx v1.4.0
	github.com/lib/pq v1.12.3
	github.com/neo4j/neo4j-go-driver/v5 v5.28.4
	github.com/redis/go-redis/v9 v9.21.0
	github.com/stretchr/testify v1.11.1
	google.golang.org/grpc v1.83.0
)

require (
	github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream v1.7.15 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.4.33 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.7.33 // indirect
	github.com/aws/aws-sdk-go-v2/internal/v4a v1.4.34 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.13.14 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/checksum v1.9.26 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.13.33 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/s3shared v1.19.34 // indirect
	github.com/aws/smithy-go v1.27.5 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	go.uber.org/atomic v1.11.0 // indirect
	golang.org/x/net v0.55.0 // indirect
	golang.org/x/sys v0.45.0 // indirect
	golang.org/x/text v0.39.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260526163538-3dc84a4a5aaa // indirect
	google.golang.org/protobuf v1.36.11 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
