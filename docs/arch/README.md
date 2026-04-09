# Architecture Pack

This folder is the operational architecture map for the current Continuo microservice system.

Use it in this order:

1. [01-topology.md](/Users/simonecarolini/Desktop/github/continuo/.wrkt/docs/arch/01-topology.md)
   High-level static view: services, owned datastores, major streams, and external systems.
2. [02-interaction-matrix.md](/Users/simonecarolini/Desktop/github/continuo/.wrkt/docs/arch/02-interaction-matrix.md)
   Fast reference for who calls what, who owns what, and which direction data moves.
3. [03-sequence-flows.md](/Users/simonecarolini/Desktop/github/continuo/.wrkt/docs/arch/03-sequence-flows.md)
   Dynamic behavior: startup, steady-state execution, retry, and rerun.
4. [04-service-ownership.md](/Users/simonecarolini/Desktop/github/continuo/.wrkt/docs/arch/04-service-ownership.md)
   Compact ownership sheet: owned durable state, owned gRPC server surface, and Redis stream roles.
5. `services/`
   Detailed dossier for each service: purpose, storage, Redis, gRPC, S3, and side effects.

This pack is based on:

- The current code under this repository
- The current multi-repo architectural snapshot
- Direct inspection of Redis, gRPC, Neo4j, Postgres, Kubernetes, and S3 integration points

Important current observations:

- `state` is the source of truth for scheduler/task/task-execution state in Postgres.
- `graph` owns dependency topology and run projections in Neo4j.
- `manifest-controller` consumes `update.graph:v1`, but no in-repo producer for that stream was found.
- `k8s-controller` produces `task.failed:v1`, but no in-repo consumer for that stream was found.
- `ui-service` is read-only and fronts `state` and `graph` over HTTP.

Service dossiers:

- [state.md](/Users/simonecarolini/Desktop/github/continuo/.wrkt/docs/arch/services/state.md)
- [graph.md](/Users/simonecarolini/Desktop/github/continuo/.wrkt/docs/arch/services/graph.md)
- [startup-controller.md](/Users/simonecarolini/Desktop/github/continuo/.wrkt/docs/arch/services/startup-controller.md)
- [dependency-controller.md](/Users/simonecarolini/Desktop/github/continuo/.wrkt/docs/arch/services/dependency-controller.md)
- [executor-controller.md](/Users/simonecarolini/Desktop/github/continuo/.wrkt/docs/arch/services/executor-controller.md)
- [k8s-controller.md](/Users/simonecarolini/Desktop/github/continuo/.wrkt/docs/arch/services/k8s-controller.md)
- [manifest-controller.md](/Users/simonecarolini/Desktop/github/continuo/.wrkt/docs/arch/services/manifest-controller.md)
- [ui-service.md](/Users/simonecarolini/Desktop/github/continuo/.wrkt/docs/arch/services/ui-service.md)
