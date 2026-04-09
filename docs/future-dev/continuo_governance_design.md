# Continuo Governance Layer

> Design Document · Engineering · Draft

---

## 1. Motivation

Continuo orchestrates data pipelines but currently has no model of ownership, criticality, or contractual obligations between nodes. As pipelines increasingly feed regulatory outputs (e.g. Bank of England submissions), an uncontrolled schema change anywhere in the transitive upstream can silently corrupt a compliance report.

The governance layer embeds three producer/consumer principles directly into the orchestration graph:

- **Ownership** — every node has a declared owner, machine-resolvable to a Slack channel and email.
- **Criticality** — nodes are classified by their business importance. This determines what obligations a schema change triggers, not whether a contract is required.
- **Contracts** — schema is declared, versioned, and enforced for every node in the platform, regardless of criticality.

---

## 2. The Universal Contract Requirement

**Every dbt model deployed to production must have a valid, registered ODCS contract in GovernanceService. No exceptions.**

This is enforced at the CI/CD layer as a blocking step. A model cannot reach production without it. The contract push is not a governance feature teams opt into — it is a hard deployment gate.

What varies by criticality is **what happens after the contract is pushed**: who gets notified, who must approve, and what execution privileges the node receives. The contract itself is always required.

```
Every node
    │
    ├── pushes ODCS contract on every deploy  ← universal, blocking
    │
    └── criticality determines:
            ├── who is notified on schema change
            ├── whether approval is required
            └── execution priority lane
```

### 2.1 What Teams Push

The ODCS payload is derived from the dbt manifest. Teams write a thin CI step that extracts the relevant fields and posts to GovernanceService. The governance service owns the canonical schema — it does not couple to dbt's manifest format directly. The dbt manifest is also included in the push so GovernanceService can extract compiled SQL for column lineage analysis.

Mandatory fields on every submission:

- `owner` — must resolve in the owner registry to a Slack channel ID and email list
- `customProperties.nodeCriticality` — `RegulatoryPath` | `CorePath` | `SecondaryPath`
- `customProperties.slackChannelId` — machine-readable Slack channel ID, not display name
- `customProperties.emailList` — distribution list for the owning team
- Full `schema` array — every column with `dataType`, `maxLength` / `precision`, and `isNullable`
- `compiled_code` — compiled SQL from the dbt manifest, used for column lineage extraction

A failed submission (malformed payload, missing fields, unresolvable owner) fails the CI/CD pipeline. The model does not deploy.

```json
{
  "id": "urn:continuo:analytics:public:orders",
  "version": "1.2.0",
  "kind": "DataContract",
  "owner": "data-platform",
  "schema": [
    { "name": "order_id",   "dataType": "string",    "maxLength": 36,  "isNullable": false },
    { "name": "amount",     "dataType": "decimal",   "precision": 18,  "isNullable": false },
    { "name": "created_at", "dataType": "timestamp",                   "isNullable": false }
  ],
  "customProperties": {
    "nodeCriticality": "RegulatoryPath",
    "slackChannelId":  "C0123ABCDEF",
    "emailList":       "data-platform@company.com"
  }
}
```

---

## 3. Node Criticality

Criticality is a property of a node that describes its business importance and determines the governance obligations that apply when its schema changes. It does not affect whether a contract is required — that is always required.

| Level | Tag | Schema Change Behaviour | Execution |
|---|---|---|---|
| Regulatory | `RegulatoryPath` | Breaking changes require approval from affected downstream `RegulatoryPath` owners. Non-breaking changes notify them. | Preferential lane — highest Kubernetes PriorityClass. |
| Core | `CorePath` | Breaking changes notify affected downstream `CorePath` and `RegulatoryPath` owners. No approval gate. | Standard execution. |
| Secondary | `SecondaryPath` | Schema changes are versioned and stored. No notifications triggered. | Standard execution. |

> **Key asymmetry:** Upstream owners are never asked to approve changes to their own node. The producer owns their node. Downstream `RegulatoryPath` owners gate breaking changes from upstream — they confirm the change does not compromise their output.

### 3.1 Classifying a Node as RegulatoryPath

When a team first tags their node as `RegulatoryPath`, GovernanceService traverses the full transitive upstream lineage using the **column-level lineage graph** (see Section 5). Only upstream teams whose nodes contribute columns that flow into the regulatory node are notified — not every upstream node in the graph.

This consent is meaningful: upstream teams are acknowledging that schema changes to the specific columns they produce may now have compliance consequences downstream.

> **Why full transitive upstream, scoped by column?**
> A casual rename of a column in a raw ingestion table four hops upstream could corrupt a Bank of England submission. Proximity does not reduce risk. But a column that is never consumed by the regulatory path is irrelevant — those teams are not pulled in.

### 3.2 Periodic Criticality Review

Every node tagged `RegulatoryPath` or `CorePath` is subject to a periodic review cycle (configurable, default: 90 days). The governance service sends a structured review request to the node owner via Slack and email asking three explicit questions:

1. Is this node still feeding a regulatory or compliance output?
2. Is the declared owner still the correct team?
3. Is the schema contract below still accurate?

Non-response after 48 hours escalates to the team lead. Non-response after 72 hours auto-confirms status quo and is audit-logged.

A "no" answer on question 1 triggers a **criticality downgrade event** — downstream owners of dependent `RegulatoryPath` nodes are notified before the downgrade takes effect.

---

## 4. Contract Versioning and Change Classification

Every push stores a new immutable contract version with an effective timestamp. Prior versions are archived, never deleted. Jobs record the contract version they were dispatched under. This provides a complete audit trail: for any job execution, you can retrieve the exact schema contract that was in force at run time.

### 4.1 Breaking vs Non-Breaking Changes

On every contract push GovernanceService diffs the incoming schema against the stored approved contract. Changes are classified automatically:

| Change Type | Examples | Action |
|---|---|---|
| **Non-breaking** | Add nullable column, increase `maxLength`, add enum value, metadata update | Notify affected downstream `RegulatoryPath` and `CorePath` owners (column-scoped). No gate. |
| **Breaking** | Remove column, rename column, reduce `maxLength`, change `dataType`, add non-nullable column, tighten nullability | Approval required from affected downstream `RegulatoryPath` owners (column-scoped). 48h SLA. |

"Affected" is determined by column lineage — only owners of downstream governed nodes that actually consume the changed column are involved. See Section 5.

### 4.2 Running Under the Last Approved Contract

A `PENDING_APPROVAL` state never blocks job dispatch. Continuo always runs under the most recently `APPROVED` contract version. Only two conditions block dispatch:

- The node has never had an approved contract (first submission was rejected).
- The approved contract has been withdrawn with no fallback version.

---

## 5. Column-Level Lineage

Node-level lineage alone would cause significant notification noise — every upstream team in a transitive chain would be involved in every approval workflow, regardless of whether the specific column that changed has any bearing on the regulated output. Column-level lineage scopes all notifications and approval workflows to exactly the teams and nodes materially affected by a given change.

GovernanceService builds and maintains a column-level lineage graph for **every node in the platform, regardless of criticality**.

### 5.1 How Column Lineage is Extracted

At contract push time, GovernanceService uses [sqlglot](https://github.com/tobymao/sqlglot) to parse the compiled SQL from the dbt manifest and resolve which source columns each output column traces back to. sqlglot operates on the compiled output — not the dbt source — so Jinja macros, `for` loops, and compile-time templating are already fully resolved before parsing.

sqlglot requires three inputs, all assembled from GovernanceService's own store:

1. **Compiled SQL of the incoming model** — from the manifest in the contract push
2. **Compiled SQL of all upstream models** — from previously stored contracts in GovernanceService
3. **Schemas of raw source tables** — from stored ODCS contracts

```python
from sqlglot.lineage import lineage

node = lineage(
    column="amount",
    sql="select order_id, amount, country from orders_enriched where country = 'GB'",
    schema={
        "orders":          {"order_id": "VARCHAR", "amount": "DECIMAL", "customer_id": "VARCHAR"},
        "customers":       {"customer_id": "VARCHAR", "email": "VARCHAR", "country": "VARCHAR"},
        "orders_enriched": {"order_id": "VARCHAR", "amount": "DECIMAL", "country": "VARCHAR"}
    },
    sources={
        "orders_enriched": """
            select o.order_id, o.amount, c.country
            from orders o
            join customers c on o.customer_id = c.customer_id
        """
    },
    dialect="snowflake"
)
# result: amount <- orders_enriched.amount <- orders.amount
```

This runs for every output column of the model being pushed. The results are written to Neo4j as `Column` nodes and `SOURCED_FROM` edges, replacing the previous lineage for that node.

### 5.2 Graph Model in Neo4j

Column lineage extends the existing `Table` / `DEPENDS_ON` graph:

```
(:Table {id: 'analytics:public:regulatory_report', criticality: 'RegulatoryPath'})
  -[:HAS_COLUMN]->
(:Column {node_id: 'analytics:public:regulatory_report', name: 'amount'})
  -[:SOURCED_FROM {lineage_confidence: 'complete'}]->
(:Column {node_id: 'analytics:public:orders_enriched', name: 'amount'})
  -[:SOURCED_FROM {lineage_confidence: 'complete'}]->
(:Column {node_id: 'raw:public:orders', name: 'amount'})
```

`lineage_confidence` is either `complete` (sqlglot fully resolved the column) or `partial` (sqlglot could not resolve — typically due to `dbt_utils.star()` or plugin-generated column lists). Partial confidence edges fall back to node-level notification for that segment.

### 5.3 Querying Affected Owners on a Column Change

When a column changes, a single Cypher query returns exactly which governed node owners to notify:

```cypher
MATCH path = (source:Column {node_id: $node_id, name: $changed_column})
             <-[:SOURCED_FROM*]-(downstream:Column)
MATCH (downstream)<-[:HAS_COLUMN]-(table:Table)
WHERE table.criticality IN ['RegulatoryPath', 'CorePath']
RETURN DISTINCT
    table.owner,
    table.node_id,
    downstream.name,
    ALL(r IN relationships(path)
        WHERE r.lineage_confidence = 'complete') AS fully_resolved
```

`fully_resolved = false` means at least one `partial` edge exists in the path. GovernanceService falls back to node-level notification for those nodes to avoid silently missing an impact.

### 5.4 When Lineage is Rebuilt

Column lineage for a node is rebuilt every time that node pushes a new contract. Downstream nodes do not need recomputation unless their own compiled SQL changes — which happens when they push their own contract through CI/CD enforcement.

### 5.5 dbt Plugins and star() Usage

`dbt_utils.star()` and similar plugin macros expand to explicit column lists at compile time by querying the warehouse catalog. The compiled SQL is static and parseable. However, the expansion is a snapshot taken at compile time — if an upstream table gains a column and the downstream model is not recompiled, the stored lineage is stale until a new contract is pushed.

GovernanceService detects `SELECT *` patterns or unexpanded star expansions in compiled SQL and marks those `SOURCED_FROM` edges as `lineage_confidence: partial`. This is surfaced in the periodic review cycle and included in the Slack notification when node-level fallback is triggered, so teams are aware their lineage precision is degraded.

### 5.6 Why All Nodes, Not Just Governed Ones

Column lineage is built for every node regardless of criticality because:

- A `SecondaryPath` node today may be reclassified tomorrow — the lineage graph is ready instantly with no rebuild.
- A gap in an intermediate `SecondaryPath` node would be a silent blind spot without lineage for all nodes. With it, the gap is a flagged `partial` edge rather than an unknown unknown.
- Full platform impact analysis becomes a native capability — any team can query what a column change touches before making it.

---

## 6. Approval State Machine

A breaking change on a node that has downstream `RegulatoryPath` dependents — identified via column lineage — enters the following state machine. The pending contract is staged; jobs continue to run under the last approved contract version until the new one is approved.

```
PENDING_APPROVAL
  → all required approvals received        → APPROVED
  → any rejection                          → REJECTED (deployment blocked)
  → 48h no response                        → escalate to team lead (+24h)
  → 72h no response                        → AUTO_APPROVED  [audit logged]

APPROVED
  → upstream requests partial withdrawal   → PARTIAL_WITHDRAWAL_PENDING
    (withdraw from specific downstream node)
  → upstream requests full withdrawal      → FULL_WITHDRAWAL_PENDING
    (deprecation — withdraw from all downstream)

PARTIAL_WITHDRAWAL_PENDING
  → downstream RegulatoryPath owner approves  → PARTIALLY_WITHDRAWN
       in-flight jobs finish under current contract
       new dispatch blocked for affected nodes until re-approval
  → downstream owner rejects               → APPROVED  (withdrawal denied)
  → 48h no response                        → escalate → 72h → AUTO_APPROVED_WITHDRAWAL

FULL_WITHDRAWAL_PENDING  (deprecation)
  → each downstream RegulatoryPath owner acts independently
  → owner approves  → that node moves to WITHDRAWN
  → owner rejects   → that node stays APPROVED
                       upstream escalates to governance owner
                       [no auto-approve on deprecation]
```

---

## 7. Integration with the Execution Pipeline

### 7.1 Priority Lane — dependency-controller

The dependency-controller sorts unblocked nodes by criticality before publishing to `query.model:v1` and tags each event with a `priority` field. Executor-controller maps this to a Kubernetes PriorityClass at job creation. The infrastructure handles preemption — no custom queue logic required.

```
dependency-controller
  → sorts unblocked nodes: RegulatoryPath first, then CorePath, then SecondaryPath
  → tags event: { priority: 'regulatory' | 'core' | 'secondary' }
  → publishes to query.model:v1

executor-controller
  → reads event
  → for RegulatoryPath and CorePath: calls GovernanceService.CheckDispatchEligibility()
  → maps priority tag → Kubernetes PriorityClass
  → creates K8s Job with contract_version recorded on the job
```

### 7.2 Dispatch Gate — CheckDispatchEligibility

Executor-controller calls `CheckDispatchEligibility` for `RegulatoryPath` and `CorePath` nodes only. `SecondaryPath` nodes bypass this call entirely. This is a governance gate only — it does not influence priority ordering, which is determined upstream by the dependency-controller sort.

| Response | Meaning |
|---|---|
| `ELIGIBLE` + `contract_version` | Node has an approved contract. Dispatch proceeds. Contract version recorded on the job. |
| `BLOCKED` | No approved contract exists or approved contract has been withdrawn. Dispatch does not proceed. State service updated. Owner notified. |

> **Fail closed:** If GovernanceService is unreachable, `RegulatoryPath` and `CorePath` dispatch is blocked. `SecondaryPath` nodes proceed normally. A governance outage does not halt the entire platform — only governed nodes are affected.

---

## 8. GovernanceService

A new service in the Continuo platform. Two runtime processes share one PostgreSQL database. Column lineage is stored in the shared Neo4j instance.

### 8.1 Contract Push Processing (synchronous, per push)

```
POST /contracts
  1. Validate ODCS payload
       reject if: malformed, missing mandatory fields, owner not in registry
  2. Diff schema against stored approved contract
       classify each changed column as non-breaking or breaking
  3. Store new contract version (immutable, effective timestamp)
  4. Assemble sqlglot inputs from GovernanceService store
  5. Run sqlglot lineage per output column
  6. Write Column nodes and SOURCED_FROM edges to Neo4j
       mark partial where sqlglot returned unresolved
  7. Query Neo4j column lineage graph for affected downstream governed nodes
  8a. Breaking change  → initiate approval workflow for affected RegulatoryPath owners
  8b. Non-breaking     → send notification to affected RegulatoryPath and CorePath owners
```

### 8.2 API Surface

- `POST /contracts` — contract push from CI/CD
- `GET /contracts/{node_id}/eligibility` — dispatch gate called by executor-controller
- `POST /approvals/{id}/approve|reject|withdraw` — Slack interactive callback handler

### 8.3 Background Process (async, scheduled)

- **SLA timer** — polls pending approvals, triggers escalation at 48h, auto-approve at 72h
- **Periodic review scheduler** — every 90 days, sends review requests for `RegulatoryPath` and `CorePath` nodes
- **Notification dispatcher** — sends Slack messages and emails. Thin adapter layer — Slack and email are the only channels in v1.

### 8.4 Data Model (PostgreSQL)

| Table | Purpose |
|---|---|
| `contracts` | Versioned ODCS payloads. Immutable rows. Effective timestamp range per version. |
| `approval_workflows` | One row per approval event. Tracks state machine, SLA timestamps, escalation state. |
| `approval_participants` | Per-participant approval status (`approved` / `rejected` / `pending`) for each workflow. |
| `owner_registry` | Maps logical owner string → Slack channel ID + email list. Validated at push time. |
| `job_contract_audit` | Records `contract_version` against every dispatched job. Append-only. |
| `review_schedule` | Tracks next review due date per node. Updated on approval and periodic confirmation. |

Column lineage (`Column` nodes and `SOURCED_FROM` edges) is stored in **Neo4j**, not PostgreSQL. GovernanceService owns both write paths.

---

## 9. Out of Scope (v1)

- **Contract drift detection** — declared schema vs actual warehouse schema. Additive feature, does not affect v1 design.
- **Structural change governance** — model splits, merges, grain changes. Owner's responsibility to manage criticality on restructure.
- **GovernanceService UI** — Slack interactive callbacks are the primary approval surface in v1.
