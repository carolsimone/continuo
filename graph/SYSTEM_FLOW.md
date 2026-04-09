# Graph Service - System Flow

## Architecture Overview

```
┌─────────────┐          ┌──────────────────────────────────┐          ┌────────────────┐
│   Client    │  gRPC    │      Graph Service :50052        │  Bolt    │     Neo4j      │
│(startup-    │─────────▶│  ┌───────────────────────────┐  │─────────▶│   Database     │
│ controller) │          │  │  Graph Handler            │  │          │   :7687        │
└─────────────┘          │  │  - Node CRUD              │  │          └────────────────┘
                         │  │  - Dependency traversal   │  │
                         │  │  - Freshness checks       │  │
                         │  └───────────────────────────┘  │
                         │                                  │
                         └──────────────────────────────────┘
                                         │
                                         │
                         ┌──────────────────────┐
                         │  Health Check :8083  │  HTTP
                         └──────────────────────┘◀──────────── Monitoring
```

## Data Flow

---

## 🔄 Node Creation Flow

### Overview
Creates or updates a table node with metadata and dependency relationships.

### Flow Diagram

```
INPUT (gRPC):
{
  "schedule_name": "daily",
  "schema_name": "public",
  "table_name": "orders",
  "service_name": "analytics",
  "owner": "data-team",
  "depends_on": [
    {"schema": "public", "table": "users"},
    {"schema": "public", "table": "products"}
  ]
}
       │
       ▼
┌──────────────────────┐
│ gRPC Handler         │
│ - Validate input     │
│ - Check required     │
│   fields             │
└──────────┬───────────┘
           │
           ▼
┌──────────────────────────────────────┐
│ Neo4j Repository                     │
│ MERGE (node:Table {                  │
│   schedule_name: $schedule,          │
│   schema_name: $schema,              │
│   table_name: $table                 │
│ })                                   │
│ SET node.service_name = $service     │
│ SET node.owner = $owner              │
│ SET node.last_update = timestamp()   │
│ SET node.created_at = timestamp()    │
│                                      │
│ // Delete old relationships          │
│ MATCH (node)-[r:DEPENDS_ON]->()      │
│ DELETE r                             │
│                                      │
│ // Create new relationships          │
│ FOR EACH dependency:                 │
│   MATCH (target:Table {              │
│     schema_name: dep.schema,         │
│     table_name: dep.table            │
│   })                                 │
│   MERGE (node)-[:DEPENDS_ON]->(target)│
└──────────┬───────────────────────────┘
           │
           ▼
OUTPUT (gRPC):
{
  "message": "Node created/updated successfully"
}
```

**Natural Language:**
Client sends table metadata → Handler validates → Repository uses MERGE (upsert) to create/update node → Deletes old dependencies → Creates new DEPENDS_ON relationships → Returns success.

**Key Points:**
- **MERGE** = upsert (create if not exists, update if exists)
- Composite key: `(schedule_name, schema_name, table_name)`
- `last_update` automatically set to current timestamp
- Dependencies replaced entirely on each update (not merged)

---

## 🔍 Get Stale Root Nodes

### Overview
Finds root nodes (no upstream dependencies) that haven't been updated within the threshold.

### Flow Diagram

```
INPUT (gRPC):
{
  "schedule_name": "daily",
  "hours_threshold": 24
}
       │
       ▼
┌──────────────────────┐
│ gRPC Handler         │
│ - Validate threshold │
│   (must be > 0)      │
└──────────┬───────────┘
           │
           ▼
┌──────────────────────────────────────────────┐
│ Neo4j Repository                             │
│ MATCH (node:Table {                          │
│   schedule_name: $schedule                   │
│ })                                           │
│ WHERE NOT (node)<-[:DEPENDS_ON]-()           │
│   // No upstream dependencies = ROOT         │
│   AND (                                      │
│     node.last_update IS NULL OR              │
│     duration.inSeconds(                      │
│       datetime(),                            │
│       datetime({epochMillis: node.last_update})│
│     ).hours > $hours_threshold               │
│   )                                          │
│ RETURN node                                  │
│ ORDER BY node.last_update ASC                │
└──────────┬───────────────────────────────────┘
           │
           ▼
OUTPUT (gRPC):
{
  "nodes": [
    {
      "schedule_name": "daily",
      "schema_name": "public",
      "table_name": "users",
      "service_name": "ingestion",
      "owner": "platform-team",
      "last_update": 1705254000000
    },
    ...
  ]
}
```

**Natural Language:**
Client requests stale roots → Handler validates threshold → Repository queries for nodes with: (1) no incoming DEPENDS_ON edges (= root), (2) last_update > threshold OR null → Returns sorted list (oldest first).

**Use Case:**
DAG initialization - find which tables to execute first.

---

## 🌳 Get Downstream Dependencies

### Overview
Traverses the dependency graph to find all tables that depend on a given node.

### Flow Diagram

```
INPUT (gRPC):
{
  "schedule_name": "daily",
  "schema_name": "public",
  "table_name": "users",
  "max_depth": 3
}
       │
       ▼
┌──────────────────────┐
│ gRPC Handler         │
│ - Validate fields    │
└──────────┬───────────┘
           │
           ▼
┌──────────────────────────────────────────────┐
│ Neo4j Repository                             │
│ MATCH path = (source:Table {                 │
│   schedule_name: $schedule,                  │
│   schema_name: $schema,                      │
│   table_name: $table                         │
│ })<-[:DEPENDS_ON*1..${maxDepth}]-(dependent) │
│                                              │
│ RETURN DISTINCT dependent                    │
│ ORDER BY length(path)                        │
└──────────┬───────────────────────────────────┘
           │
           ▼
OUTPUT (gRPC):
{
  "dependencies": [
    {
      "schema_name": "analytics",
      "table_name": "user_summary",
      "service_name": "reporting"
    },
    {
      "schema_name": "ml",
      "table_name": "user_features",
      "service_name": "ml-pipeline"
    }
  ]
}
```

**Natural Language:**
Client specifies source node + max depth → Handler validates → Repository uses variable-length path pattern `*1..maxDepth` to traverse DEPENDS_ON edges backwards → Returns all downstream dependents ordered by distance.

**Use Case:**
Impact analysis - "If I update table X, which tables are affected?"

---

## ✅ Check Upstream Freshness

### Overview
Verifies all upstream dependencies have been updated within the threshold.

### Flow Diagram

```
INPUT (gRPC):
{
  "schema_name": "analytics",
  "table_name": "orders",
  "hours_threshold": 2
}
       │
       ▼
┌──────────────────────┐
│ gRPC Handler         │
│ - Validate fields    │
└──────────┬───────────┘
           │
           ▼
┌──────────────────────────────────────────────┐
│ Neo4j Repository                             │
│ MATCH (node:Table {                          │
│   schema_name: $schema,                      │
│   table_name: $table                         │
│ })                                           │
│ OPTIONAL MATCH (node)-[:DEPENDS_ON]->(upstream)│
│                                              │
│ WITH node, collect(upstream) AS upstreams   │
│                                              │
│ // Check each upstream                       │
│ WITH upstreams,                              │
│   [u IN upstreams WHERE                      │
│     u.last_update IS NULL OR                 │
│     duration.inSeconds(                      │
│       datetime(),                            │
│       datetime({epochMillis: u.last_update}) │
│     ).hours > $hours_threshold               │
│   ] AS stale_deps                            │
│                                              │
│ RETURN size(stale_deps) = 0 AS is_fresh,     │
│        stale_deps                            │
└──────────┬───────────────────────────────────┘
           │
           ▼
OUTPUT (gRPC):
{
  "is_fresh": false,
  "stale_dependencies": [
    {
      "schema_name": "public",
      "table_name": "users",
      "last_update": 1705240000000
    }
  ]
}
```

**Natural Language:**
Client requests freshness check for node → Handler validates → Repository finds all direct upstream dependencies → Checks each upstream's last_update against threshold → Returns boolean + list of stale dependencies.

**Use Case:**
Pre-execution validation - "Are all my dependencies ready before I run?"

---

## 📊 Update Node Timestamp

### Overview
Updates the last_update timestamp after successful table execution.

### Flow Diagram

```
INPUT (gRPC):
{
  "schema_name": "public",
  "table_name": "orders"
}
       │
       ▼
┌──────────────────────┐
│ gRPC Handler         │
│ - Validate fields    │
└──────────┬───────────┘
           │
           ▼
┌──────────────────────────────────────────────┐
│ Neo4j Repository                             │
│ MATCH (node:Table {                          │
│   schema_name: $schema,                      │
│   table_name: $table                         │
│ })                                           │
│ SET node.last_update = timestamp()           │
│ RETURN node                                  │
└──────────┬───────────────────────────────────┘
           │
           ▼
OUTPUT (gRPC):
{
  "message": "Timestamp updated successfully"
}
```

**Natural Language:**
Client notifies completion of table → Handler validates → Repository finds node and updates last_update to current timestamp → Returns success or NotFound error.

**Use Case:**
Post-execution tracking - mark table as fresh after successful run.

---

## Data Model

### Node Label: Table

```cypher
CREATE (:Table {
  schedule_name: string,    // Schedule context (e.g., "daily", "hourly")
  schema_name: string,      // Database schema
  table_name: string,       // Table name
  service_name: string,     // Service responsible for this table
  owner: string,            // Team/person owner
  last_update: long,        // Timestamp (epoch millis) of last update
  created_at: long          // Timestamp (epoch millis) of node creation
})

// Composite unique key
CREATE CONSTRAINT FOR (t:Table)
  REQUIRE (t.schedule_name, t.schema_name, t.table_name) IS UNIQUE
```

### Relationship: DEPENDS_ON

```cypher
(:Table)-[:DEPENDS_ON]->(:Table)

// Direction: source depends on target
// Example: orders depends on users
(orders:Table)-[:DEPENDS_ON]->(users:Table)
```

**Traversal Patterns:**

- **Find upstream (parents)**: `(node)-[:DEPENDS_ON]->(upstream)`
- **Find downstream (children)**: `(node)<-[:DEPENDS_ON]-(downstream)`
- **Find roots**: `(node:Table) WHERE NOT (node)-[:DEPENDS_ON]->()`
- **Find leaves**: `(node:Table) WHERE NOT (node)<-[:DEPENDS_ON]-()`

---

## Component Interaction

```
┌──────────────────┐
│ Startup          │
│ Controller       │
└────────┬─────────┘
         │
         │ 1. GetStaleRootNodes("daily", 24)
         ▼
┌──────────────────┐
│ Graph Service    │◀─────── Returns: [users, products, events]
└────────┬─────────┘
         │
         │ 2. For each root node, create tasks
         ▼
┌──────────────────┐
│ State Service    │
│ CreateTask()     │
└────────┬─────────┘
         │
         │ 3. Execute tasks
         ▼
┌──────────────────┐
│ Executor Service │
└────────┬─────────┘
         │
         │ 4. On success, update timestamp
         ▼
┌──────────────────┐
│ Graph Service    │
│ UpdateNodeTimestamp()
└──────────────────┘
         │
         │ 5. Check downstream nodes for readiness
         ▼
┌──────────────────┐
│ Graph Service    │
│ CheckUpstreamFreshness() → If fresh, queue for execution
└──────────────────┘
```

---

## Error Handling

### Node Not Found
```
CheckUpstreamFreshness(schema="missing", table="table")
→ gRPC error: NotFound
→ Client should handle by checking node exists
```

### Invalid Threshold
```
GetStaleRootNodes(hours_threshold=-1)
→ gRPC error: InvalidArgument
→ "hours_threshold must be positive"
```

### Neo4j Connection Lost
```
CreateNode(...)
→ gRPC error: Unavailable
→ "database connection error"
→ Client should retry with exponential backoff
```

---

## Performance Considerations

### Indexing Strategy

```cypher
// Composite index for node lookups
CREATE INDEX idx_table_composite
  FOR (t:Table)
  ON (t.schedule_name, t.schema_name, t.table_name)

// Index for freshness queries
CREATE INDEX idx_table_last_update
  FOR (t:Table)
  ON (t.last_update)
```

### Query Optimization

- **Use MERGE for upserts**: More efficient than separate CHECK + CREATE/UPDATE
- **Limit traversal depth**: Always specify max_depth to prevent runaway queries
- **Use DISTINCT**: Remove duplicate paths in dependency traversal
- **Profile queries**: Use `PROFILE` to analyze query performance

---

## Testing

All tests use Neo4j testcontainers for isolation:

```go
// Setup test Neo4j instance
neo4jContainer := testcontainers.Neo4jContainer{
    ImageVersion: "5.15.0-community",
}

// Run tests against isolated instance
// Each test gets fresh database
```

**Test Coverage:**
- ✅ Node creation with/without dependencies
- ✅ Timestamp updates
- ✅ Stale root detection
- ✅ Downstream dependency traversal with max depth
- ✅ Upstream freshness validation
- ✅ Error handling (missing nodes, invalid params)

---

## License

Internal use only.
