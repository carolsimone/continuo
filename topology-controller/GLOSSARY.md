# topology-controller

topology-controller reads a candidate release's projects into its topology and data contracts: which nodes it holds, which tables each node reads, and which node builds each of them. The shared words (node, project, topology, data contract, relation, ontology, unknown-external node, dormant node) are in the root `GLOSSARY.md`.

## Language

### Reading a release

**Parse**:
Reading every project of a candidate release into one topology, with a data contract for each node. It is the release check's parse leg.
_Avoid_: load, ingest, compile

**Cross-project read**:
A node that reads a table another project's node builds. The parse finds it from the node's SQL, since dbt cannot see across projects.
_Avoid_: cross-service dependency, external ref

**Excluded model**:
A dbt model, seed or snapshot the parse leaves out, so it is never a node: one with no owner, a local stub, or an untagged one from an installed package.
_Avoid_: skipped node, ignored model

**Local stub**:
A model that stands in, during a team's own work, for a table it does not really build, such as another project's table. The parse leaves it out.
_Avoid_: mock model, fake table

**Installed package**:
A dbt package a project installs. Its models are never nodes of the project.
_Avoid_: dependency package, vendored model

**System relation**:
A table the warehouse itself provides, such as its catalog of tables. A node may read it, and it never becomes an unknown-external node.
_Avoid_: system table, built-in table

### Data contracts

**Generated data contract**:
The data contract the parse writes for a dbt node from the node and its tests. A Python node's data contract is the one its team wrote.
_Avoid_: generated contract, derived contract, auto contract

**Ontology finding**:
A warning or a note from checking a release's bindings against the ontology. It never rejects the release; an error does.
_Avoid_: ontology warning, lint result
