# Graph Service - Diagram Flow Documentation

## Service Overview

The Graph Service is a gRPC-based microservice that manages a dependency graph of data tables using Neo4j. It tracks table metadata, dependencies, and freshness status.

**Technology Stack:**
- Protocol: gRPC (Protocol Buffers)
- Database: Neo4j 5.15.0 (Graph Database)
- Language: Go 1.25.1
- Ports: 50052 (gRPC), 8083 (Health Check)

---

## Architecture Diagram

```mermaid
graph TB
    Client[gRPC Client] -->|Port 50052| GS[Graph Service]
    HC[Health Check] -->|Port 8083| HTTP[HTTP Server]

    GS -->|CRUD Operations| Handler[Graph Handler]
    Handler -->|Business Logic| Repo[Graph Repository]
    Repo -->|Cypher Queries| Neo4j[(Neo4j Database)]

    Neo4j -->|Stores| Nodes[Table Nodes]
    Neo4j -->|Stores| Rels[DEPENDS_ON Relationships]

    style GS fill:#2196F3,color:#fff
    style Neo4j fill:#4CAF50,color:#fff
    style Handler fill:#FF9800,color:#fff
    style Repo fill:#9C27B0,color:#fff
```

---

## Data Model

```mermaid
erDiagram
    TABLE {
        string table_name PK
        string schema PK
        string service_name
        string owner
        string schedule_name
        string criticality
        datetime last_updated_at
        datetime created_at
    }

    TABLE ||--o{ TABLE : "DEPENDS_ON"
```

**Node Properties:**
- `table_name` + `schema` = Composite Primary Key
- `criticality` = REGULATORY | CORE | SECONDARY
- `schedule_name` = daily | hourly | etc.

**Relationship:**
- `(downstream:Table)-[:DEPENDS_ON]->(upstream:Table)`

---

## Service Endpoints

### 1. CreateNode

Creates or updates a table node with its upstream dependencies.

```mermaid
sequenceDiagram
    participant C as Client
    participant H as Handler
    participant R as Repository
    participant N as Neo4j

    C->>H: CreateNodeRequest
    Note over C,H: table_name, schema_name<br/>service_name, owner<br/>schedule_name, criticality<br/>upstream_dependencies[]

    H->>H: Validate Request
    alt Validation Failed
        H-->>C: InvalidArgument Error
    end

    H->>R: CreateNode(request)
    R->>N: MERGE Table Node
    N->>N: Set Properties
    N->>N: Delete Old Dependencies
    N->>N: Create New Dependencies
    N-->>R: Table Node
    R-->>H: TableNode
    H-->>C: NodeResponse
    Note over H,C: TableNode with<br/>all properties
```

**Input:**
```protobuf
message CreateNodeRequest {
  string table_name           // Required
  string schema_name          // Required
  string service_name         // Required
  string owner                // Required
  string schedule_name        // Required
  Criticality criticality     // Required
  repeated UpstreamDependency upstream_dependencies  // Optional
}
```

**Output:**
```protobuf
message NodeResponse {
  TableNode node
}
```

**Cypher Query (No Dependencies):**
```cypher
MERGE (t:Table {schema: $schema, table_name: $table_name})
ON CREATE SET
  t.service_name = $service_name,
  t.owner = $owner,
  t.schedule_name = $schedule_name,
  t.criticality = $criticality,
  t.created_at = datetime(),
  t.last_updated_at = datetime()
ON MATCH SET
  t.service_name = $service_name,
  t.owner = $owner,
  t.schedule_name = $schedule_name,
  t.criticality = $criticality,
  t.last_updated_at = datetime()
WITH t
OPTIONAL MATCH (t)-[old:DEPENDS_ON]->()
DELETE old
RETURN t
```

**Cypher Query (With Dependencies):**
```cypher
-- Same as above, plus:
WITH t
UNWIND $upstream_dependencies AS dep
MERGE (upstream:Table {schema: dep.schema_name, table_name: dep.table_name})
MERGE (t)-[:DEPENDS_ON]->(upstream)
RETURN t
```

---

### 2. UpdateNodeTimestamp

Updates only the `last_updated_at` timestamp of a table node.

```mermaid
sequenceDiagram
    participant C as Client
    participant H as Handler
    participant R as Repository
    participant N as Neo4j

    C->>H: UpdateNodeTimestampRequest
    Note over C,H: table_name<br/>schema_name

    H->>H: Validate Request
    alt Validation Failed
        H-->>C: InvalidArgument Error
    end

    H->>R: UpdateNodeTimestamp(schema, table)
    R->>N: MATCH + SET last_updated_at

    alt Node Not Found
        N-->>R: No Results
        R-->>H: ErrNotFound
        H-->>C: NotFound Error
    else Node Found
        N-->>R: Updated Node
        R-->>H: TableNode
        H-->>C: NodeResponse
    end
```

**Input:**
```protobuf
message UpdateNodeTimestampRequest {
  string table_name   // Required
  string schema_name  // Required
}
```

**Output:**
```protobuf
message NodeResponse {
  TableNode node
}
```

**Cypher Query:**
```cypher
MATCH (t:Table {schema: $schema, table_name: $table_name})
SET t.last_updated_at = datetime()
RETURN t
```

---

### 3. GetStaleRootNodes

Retrieves root nodes (tables without upstream dependencies) that haven't been updated in the last N hours.

```mermaid
sequenceDiagram
    participant C as Client
    participant H as Handler
    participant R as Repository
    participant N as Neo4j

    C->>H: GetStaleRootNodesRequest
    Note over C,H: hours_threshold

    H->>H: Validate threshold > 0
    alt Validation Failed
        H-->>C: InvalidArgument Error
    end

    H->>R: GetStaleRootNodes(hours)
    R->>N: MATCH root nodes WHERE stale
    Note over N: Find nodes with:<br/>- No DEPENDS_ON relationships<br/>- last_updated_at < now - hours
    N-->>R: List of Nodes
    R-->>H: []TableNode
    H-->>C: GetStaleRootNodesResponse
    Note over H,C: List of stale<br/>root nodes
```

**Input:**
```protobuf
message GetStaleRootNodesRequest {
  int32 hours_threshold  // Required: > 0
}
```

**Output:**
```protobuf
message GetStaleRootNodesResponse {
  repeated TableNode nodes
}
```

**Cypher Query:**
```cypher
MATCH (t:Table)
WHERE NOT (t)-[:DEPENDS_ON]->()
  AND t.last_updated_at < datetime() - duration({hours: $hours})
RETURN t
ORDER BY t.last_updated_at ASC
```

**Use Case:** Find starting points for data pipeline execution (tables with no dependencies that need processing).

---

### 4. GetDownstreamDependencies

Retrieves all downstream dependencies (tables that depend on the specified table) within a given schedule and optional depth limit.

```mermaid
sequenceDiagram
    participant C as Client
    participant H as Handler
    participant R as Repository
    participant N as Neo4j

    C->>H: GetDownstreamDependenciesRequest
    Note over C,H: schedule_name, schema_name<br/>table_name, max_depth

    H->>H: Validate Required Fields
    alt Validation Failed
        H-->>C: InvalidArgument Error
    end

    H->>R: GetDownstreamDependencies(...)
    R->>N: MATCH path with DEPENDS_ON
    Note over N: Traverse graph:<br/>Find all tables that<br/>depend on this table<br/>(directly or indirectly)
    N-->>R: List of Dependencies with Depth
    R-->>H: []Dependency
    H-->>C: GetDownstreamDependenciesResponse
    Note over H,C: Dependencies ordered<br/>by depth
```

**Input:**
```protobuf
message GetDownstreamDependenciesRequest {
  string schedule_name  // Required
  string table_name     // Required
  string schema_name    // Required
  int32 max_depth       // Optional: 0 = unlimited
}
```

**Output:**
```protobuf
message GetDownstreamDependenciesResponse {
  repeated Dependency dependencies
  int32 total_count
}

message Dependency {
  TableNode node
  int32 depth  // Distance from starting node
}
```

**Cypher Query:**
```cypher
MATCH (start:Table {schedule_name: $schedule_name, schema: $schema, table_name: $table_name})
MATCH path = (start)<-[:DEPENDS_ON*1..]-(downstream:Table)
RETURN DISTINCT downstream, length(path) AS depth
ORDER BY depth ASC
```

**Cypher Query (With Max Depth):**
```cypher
MATCH (start:Table {schedule_name: $schedule_name, schema: $schema, table_name: $table_name})
MATCH path = (start)<-[:DEPENDS_ON*1..3]-(downstream:Table)  -- max_depth = 3
RETURN DISTINCT downstream, length(path) AS depth
ORDER BY depth ASC
```

**Use Case:** Impact analysis - determine what tables will be affected if this table is updated or fails.

---

### 5. CheckUpstreamFreshness

Checks if all upstream dependencies of a table have been updated within a specified time threshold.

```mermaid
sequenceDiagram
    participant C as Client
    participant H as Handler
    participant R as Repository
    participant N as Neo4j

    C->>H: CheckUpstreamFreshnessRequest
    Note over C,H: schema_name, table_name<br/>freshness_minutes (default: 30)

    H->>H: Validate Required Fields
    alt Validation Failed
        H-->>C: InvalidArgument Error
    end

    H->>R: CheckUpstreamFreshness(...)
    R->>N: MATCH upstream nodes
    Note over N: Find upstreams where:<br/>last_updated_at is older<br/>than threshold
    N-->>R: List of Stale Upstreams
    R->>R: all_fresh = len(stale) == 0
    R-->>H: all_fresh, []StaleUpstream
    H-->>C: CheckUpstreamFreshnessResponse
    Note over H,C: all_fresh flag<br/>+ list of stale upstreams
```

**Input:**
```protobuf
message CheckUpstreamFreshnessRequest {
  string table_name        // Required
  string schema_name       // Required
  int32 freshness_minutes  // Optional: default 30
}
```

**Output:**
```protobuf
message CheckUpstreamFreshnessResponse {
  bool all_fresh
  int32 fresh_count
  int32 total_count
  repeated StaleUpstream stale_upstreams
}

message StaleUpstream {
  TableNode node
  int32 minutes_since_update
}
```

**Cypher Query:**
```cypher
MATCH (t:Table {schema: $schema, table_name: $table_name})-[:DEPENDS_ON]->(upstream:Table)
WITH upstream,
     duration.between(upstream.last_updated_at, datetime()).minutes AS minutes_stale
WHERE minutes_stale > $freshness_minutes
RETURN upstream, toInteger(minutes_stale) AS minutes_since_update
ORDER BY minutes_stale DESC
```

**Use Case:** Pre-execution validation - ensure all input data is ready before starting a data pipeline task.

---

## Complete Data Flow

```mermaid
graph LR
    subgraph "External Systems"
        Orchestrator[Data Orchestrator]
        Monitor[Monitoring System]
    end

    subgraph "Graph Service - Port 50052"
        CreateNode[CreateNode]
        UpdateTS[UpdateNodeTimestamp]
        GetStale[GetStaleRootNodes]
        GetDown[GetDownstreamDependencies]
        CheckUp[CheckUpstreamFreshness]
    end

    subgraph "Neo4j Database"
        Nodes[(Table Nodes)]
        Rels[(DEPENDS_ON Edges)]
    end

    %% Write Operations
    Orchestrator -->|Register Table| CreateNode
    Orchestrator -->|Update Status| UpdateTS
    CreateNode -->|MERGE| Nodes
    CreateNode -->|CREATE| Rels
    UpdateTS -->|SET timestamp| Nodes

    %% Read Operations
    Orchestrator -->|Find Work| GetStale
    Orchestrator -->|Impact Analysis| GetDown
    Orchestrator -->|Pre-check| CheckUp
    GetStale -->|MATCH| Nodes
    GetDown -->|TRAVERSE| Rels
    CheckUp -->|MATCH| Rels

    %% Monitoring
    Monitor -->|Health Check| GetStale

    style CreateNode fill:#4CAF50,color:#fff
    style UpdateTS fill:#2196F3,color:#fff
    style GetStale fill:#FF9800,color:#fff
    style GetDown fill:#9C27B0,color:#fff
    style CheckUp fill:#F44336,color:#fff
    style Nodes fill:#607D8B,color:#fff
    style Rels fill:#795548,color:#fff
```

---

## Typical Usage Scenarios

### Scenario 1: Pipeline Execution

```mermaid
sequenceDiagram
    participant Orch as Orchestrator
    participant GS as Graph Service
    participant Neo4j as Neo4j DB

    Note over Orch: Start Pipeline Execution

    Orch->>GS: GetStaleRootNodes(hours=1)
    GS->>Neo4j: Find root nodes not updated
    Neo4j-->>GS: [table1, table2, table3]
    GS-->>Orch: List of stale roots

    loop For each stale root
        Orch->>GS: CheckUpstreamFreshness(table)
        GS->>Neo4j: Check upstream dependencies
        Neo4j-->>GS: all_fresh = true
        GS-->>Orch: Ready to execute

        Orch->>Orch: Execute Pipeline

        Orch->>GS: UpdateNodeTimestamp(table)
        GS->>Neo4j: Update timestamp
        Neo4j-->>GS: Updated
        GS-->>Orch: Success

        Orch->>GS: GetDownstreamDependencies(table)
        GS->>Neo4j: Find downstream tables
        Neo4j-->>GS: [downstream1, downstream2]
        GS-->>Orch: Downstream list

        Orch->>Orch: Trigger downstream pipelines
    end
```

### Scenario 2: Table Registration

```mermaid
sequenceDiagram
    participant Admin as Admin/CI/CD
    participant GS as Graph Service
    participant Neo4j as Neo4j DB

    Admin->>GS: CreateNode(customers)
    Note over Admin,GS: table: customers<br/>schema: analytics<br/>service: dbt<br/>owner: data_team<br/>schedule: daily<br/>criticality: CORE<br/>upstreams: [raw_users, raw_orders]

    GS->>Neo4j: MERGE customers node
    Neo4j->>Neo4j: Set properties
    Neo4j->>Neo4j: Create DEPENDS_ON -> raw_users
    Neo4j->>Neo4j: Create DEPENDS_ON -> raw_orders
    Neo4j-->>GS: Created node
    GS-->>Admin: Success
```

### Scenario 3: Impact Analysis

```mermaid
sequenceDiagram
    participant Ops as Operations Team
    participant GS as Graph Service
    participant Neo4j as Neo4j DB

    Note over Ops: raw_users table failed

    Ops->>GS: GetDownstreamDependencies(raw_users)
    GS->>Neo4j: MATCH downstream dependencies
    Neo4j-->>GS: [customers (depth=1), orders (depth=1), revenue (depth=2)]
    GS-->>Ops: Affected tables with depth

    Note over Ops: Alert: 3 tables affected!<br/>Assess impact and plan recovery
```

---

## Error Handling

```mermaid
graph TD
    Request[gRPC Request] --> Validate{Validation}

    Validate -->|Missing Fields| InvalidArg[InvalidArgument Error]
    Validate -->|Invalid Values| InvalidArg
    Validate -->|OK| Execute[Execute Operation]

    Execute --> DB{Database Operation}

    DB -->|Not Found| NotFound[NotFound Error]
    DB -->|Query Error| Internal[Internal Error]
    DB -->|Success| Response[Success Response]

    InvalidArg --> Client[Return to Client]
    NotFound --> Client
    Internal --> Client
    Response --> Client

    style InvalidArg fill:#FFC107,color:#000
    style NotFound fill:#FF9800,color:#fff
    style Internal fill:#F44336,color:#fff
    style Response fill:#4CAF50,color:#fff
```

**Error Codes:**
- `InvalidArgument` - Missing or invalid request parameters
- `NotFound` - Table node not found in database
- `Internal` - Database or server errors

---

## Performance Characteristics

| Operation | Complexity | Index Usage | Typical Response Time |
|-----------|-----------|-------------|---------------------|
| CreateNode | O(1) + O(n) deps | Primary Key | 50-100ms |
| UpdateNodeTimestamp | O(1) | Primary Key | 30-50ms |
| GetStaleRootNodes | O(n) | Timestamp + Index | 100-500ms |
| GetDownstreamDependencies | O(d * b^d) | Relationship Traversal | 100-1000ms |
| CheckUpstreamFreshness | O(u) | Relationship + Timestamp | 50-200ms |

**Legend:**
- n = number of upstream dependencies
- d = depth of traversal
- b = average branching factor
- u = number of upstream dependencies

**Indexes Required:**
- Composite index on `(schema, table_name)` - Primary lookup
- Index on `last_updated_at` - Stale node queries
- Index on `schedule_name` - Downstream queries

---

## Health Check

**Endpoint:** `GET /health` (Port 8083)

**Response:**
```json
{
  "status": "healthy",
  "service": "graph"
}
```

---

## Service Dependencies

```mermaid
graph TB
    GS[Graph Service] -->|Requires| Neo4j[(Neo4j 5.15.0)]
    GS -->|Exposes| gRPC[gRPC Port 50052]
    GS -->|Exposes| HTTP[HTTP Port 8083]

    Neo4j -->|Storage| Data[(Graph Data)]

    Client1[Orchestrator] -->|Calls| gRPC
    Client2[Monitoring] -->|Calls| HTTP
    Client3[Admin Tools] -->|Calls| gRPC

    style GS fill:#2196F3,color:#fff
    style Neo4j fill:#4CAF50,color:#fff
```

**Required:**
- Neo4j 5.15.0+ (bolt://neo4j:7687)

**Optional:**
- Monitoring system (health checks)
- Orchestration system (gRPC client)

---

## Configuration

**Environment Variables:**
- `NEO4J_URI` - Neo4j connection URI (default: bolt://localhost:7687)
- `NEO4J_USER` - Neo4j username (default: neo4j)
- `NEO4J_PASSWORD` - Neo4j password (default: atlas_password)
- `GRPC_PORT` - gRPC server port (default: 50052)
- `HEALTH_PORT` - Health check HTTP port (default: 8083)
- `LOG_LEVEL` - Logging level (default: INFO)

---

## Testing

All endpoints are covered by comprehensive integration tests using testcontainers with a real Neo4j instance.

**Test Coverage:**
- ✅ CreateNode (4 test scenarios)
- ✅ UpdateNodeTimestamp (2 test scenarios)
- ✅ GetStaleRootNodes (2 test scenarios)
- ✅ GetDownstreamDependencies (3 test scenarios)
- ✅ CheckUpstreamFreshness (3 test scenarios)

**Total:** 14 integration tests, all passing

Run tests:
```bash
docker-compose exec graph bash -c "cd /app/graph && go test -v ./test/..."
```
