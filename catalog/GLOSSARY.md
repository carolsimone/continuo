# catalog

catalog keeps the graph of the live release's nodes and edges, every version of each node's code and data contract, and the failures seen at each node, so other components can read them. The shared words (node, release, live release, topology, data contract, project) are in the root `GLOSSARY.md`. Inside catalog, contract always means a data contract.

## Language

### Nodes over time

**Retired node**:
A node a release dropped. Its history (code versions, failures, fixes) stays, but it is no longer in the live topology.
_Avoid_: deleted node, inactive node, removed node

**Code version**:
One version of a node's code as a release carried it, both as written and as compiled.
_Avoid_: revision, snapshot, commit

**Current version**:
The code version of a node the live release runs.
_Avoid_: latest version, head

**Failure precedent**:
An earlier failure at a node, kept with the fix proposed for it and whether that fix was merged, so a later fix can learn from it.
_Avoid_: case, rejection record, incident

### Contracts over time

**Contract version**:
One version of a node's data contract, recorded at the promotion that published it. Going back to an earlier data contract is a new version that is the same as the old one.
_Avoid_: contract revision, contract snapshot

**Live contracts**:
The published data contract of every node the live release builds, one file per node with an index, for readers outside continuo.
_Avoid_: contract registry, contract export

**Declared schema**:
The warehouse schema a node says it writes to, spelled as its project wrote it. Published data contracts name it as the place to read the table.
_Avoid_: target schema, physical schema

### Keeping the graph safe

**Backup**:
One saved copy of a tenant's whole graph, taken on a schedule or by hand. Outside catalog it is a catalog backup.
_Avoid_: export, dump, snapshot

**Restore**:
Replacing a tenant's graph with a backup.
_Avoid_: import, reload, rollback
