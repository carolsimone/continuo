# Architecture Pack

This folder is the operational architecture map for the current Continuo microservice system.

Use it in this order:

1. [01-topology.md](docs/arch/01-topology.md)
   High-level static view: services, owned datastores, major streams, and external systems.
2. [02-interaction-matrix.md](docs/arch/02-interaction-matrix.md)
   Fast reference for who calls what, who owns what, and which direction data moves.
3. [03-sequence-flows.md](docs/arch/03-sequence-flows.md)
   Dynamic behavior: startup, steady-state execution, retry, and rerun.
4. [04-service-ownership.md](docs/arch/04-service-ownership.md)
   Compact ownership sheet: owned durable state, owned gRPC server surface, and Redis stream roles.
5. `services/`
   Detailed dossier for each service: purpose, storage, Redis, gRPC, S3, and side effects.

This pack is based on:

- The current code under this repository
- The current multi-repo architectural snapshot
- Direct inspection of Redis, gRPC, Neo4j, Postgres, Kubernetes, and S3 integration points

Service dossiers:

- [state.md](docs/arch/services/state.md)
- [orchestrator.md](docs/arch/services/orchestrator.md)
- [executor-controller.md](docs/arch/services/executor-controller.md)
- [k8s-controller.md](docs/arch/services/k8s-controller.md)
- [release-controller.md](docs/arch/services/release-controller.md)
- [remediation.md](docs/arch/services/remediation.md)
- [remediation-agent.md](docs/arch/services/remediation-agent.md)
- [agent-runner.md](docs/arch/services/agent-runner.md)
- [manifest-controller.md](docs/arch/services/manifest-controller.md)
- [ui-service.md](docs/arch/services/ui-service.md)
- [cli.md](docs/arch/services/cli.md)
