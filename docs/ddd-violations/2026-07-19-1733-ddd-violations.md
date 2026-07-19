# DDD / Clean Architecture Audit — 2026-07-19 17:33

**Scope:** branch diff vs main (commits `21b80340..HEAD`, 30 commits, 94 files)
**Branch / commit:** `worktree-parse-free-dbt-jobs` @ `77d26420`
**Summary:** 0 blockers, 3 should-fix, 4 nits

## Findings

### [SHOULD-FIX] Arch doc contradicts the shipped unknown-key tolerance in commandcfg
- **Where:** `docs/arch/services/executor-controller.md:131` vs `executor-controller/adapters/commandcfg/load.go:44-60`
- **Rule:** Layer 2 rule 5 — arch-doc currency.
- **Problem:** The doc states "A file that exists but fails to parse or fails validation (**unknown operation key**, empty argv, …) is a fatal boot error." Commit `a87a5f8d` (later on this same branch) changed `commandcfg.Load` to attempt a strict `KnownFields` decode, and when the failure consists exclusively of "field X not found" errors it logs a warning and re-decodes leniently — an unknown operation key is explicitly *not* fatal anymore (rolling-deploy forward compatibility). The doc paragraph predates that commit and was not reconciled.
- **Suggested fix:** Rewrite the sentence at line 131: remove "unknown operation key" from the fatal list and document the actual behavior — unknown fields are logged (`unknown_fields`) and ignored; a block left incomplete after dropping them still fails the completeness check.

### [SHOULD-FIX] Parse-cache hydration contract literals duplicated across three codebases with no shared constant or guard test
- **Where:**
  - `executor-controller/adapters/k8s/client.go:465` (`Name: "hydrate-parse-cache"`)
  - `dbt/s3-sidecar/parse_cache_fetcher.py:28-29` (`degraded:<reason>`) and `:84` (`"hydrated"`)
  - `k8s-controller/service/handlers/check_status_handler.go:797` (`result.InitTerminationMessages["hydrate-parse-cache"]`) and `:802-805` (`"hydrated"` / `"degraded:"` prefix parsing)
- **Rule:** Layer 1 boundary erosion (implicit cross-service/cross-language string contract); repo precedent is `pkg/validationresult`, which centralizes the sentinel strings and binds the Python side with a guard test — this feature introduces an analogous contract without that treatment.
- **Problem:** Three independently-owned literals (`hydrate-parse-cache` container name, `hydrated` marker, `degraded:` prefix) must stay in lockstep across executor-controller (names the container), the Python fetcher (writes the termination message), and k8s-controller (reads both and derives `task_execution.parse_cache`). Each side pins its own literal in its own tests (`k8s-controller/adapters/k8s/client_test.go:353`, `dbt/tests/test_parse_cache_fetcher.py:32`), but nothing binds them to each other; a rename on one side compiles, passes unit tests, and silently turns every execution's `parse_cache` into `unknown`/absent. Only the (slow, cold-stack) e2e in `tests/e2e/release_promote_test.go` would catch it.
- **Suggested fix:** Add a small `pkg/parsecache` (or extend `pkg/validationresult`'s pattern): constants for the container name, the `hydrated` marker, and the `degraded:` prefix, consumed by both Go services, plus a guard test asserting `dbt/s3-sidecar/parse_cache_fetcher.py` contains the same strings. Optionally have the k8s-controller adapter derive a typed parse-cache state so the handler stops parsing raw termination-message dialect (`parseCacheFromResult` moves behind `K8sPodResult`).

### [SHOULD-FIX] Candidate-schema name assembled inline for the sixth time — now a wire contract feeding the parse leg
- **Where:** `release-controller/service/handlers/advance_queue.go:119` (`CandidateSchema: "_candidate_" + SanitizeSchemaSuffix(next.ID())`); pre-existing siblings at `handle_parsed_manifest.go:188,288`, `handle_validation_result.go:198`, `handle_seed_build_result.go:116,209`
- **Rule:** Layer 1 — clean-code/duplication; naming drift that will bite later.
- **Problem:** The branch adds the sixth inline occurrence of the `"_candidate_" + SanitizeSchemaSuffix(id)` expression, and this new one crosses a service boundary: it rides `compile.requested:v1` as `candidate_schema` and drives the `parse-candidate` rehearsal's `DBT_TARGET_SCHEMA`. If any of the six sites drifts (prefix change, different suffix function), the parse rehearsal validates a schema name the seed-build/validation legs never use — a silent cache invalidation, exactly the class of env-value drift the rehearsal gate exists to catch.
- **Suggested fix:** Extract one exported helper (e.g. `CandidateSchemaFor(releaseID string) string`) next to `SanitizeSchemaSuffix` in `release-controller/service/handlers` and use it at all six call sites.

### [NIT] S3 URI builders live in the domain layer
- **Where:** `executor-controller/domain/deploy/parse_cache.go:7,14` (`ParseCacheProdURI`, `ParseCacheCandidateURI`)
- **Rule:** Layer 1 — domain purity (S3 belongs in adapters).
- **Problem:** The two functions bake the `s3://<bucket>/<service>/...` addressing scheme into domain code. Mitigating context: they are pure string functions with no infra imports, the domain `ValidationJobSpec` already carried `ManifestS3URI`/`CandidateSQLURI` as opaque values before this branch, and placing them here is what lets both the application handler (`compile_requested_handler.go:56-57`) and the k8s adapter (`client.go:710,953`) share one canonical definition without an adapter→service import. Still, the *construction* of storage addresses (as opposed to carrying them opaquely) is infrastructure vocabulary; a `service/`-layer or `pkg/` home would be strictly cleaner. Related inconsistency: `compile_requested_handler.go:49` still builds the manifest URI by inline concatenation while the parse URIs get helpers.
- **Suggested fix:** Either accept and document the trade-off, or move the builders (plus the inline manifest-URI concatenation) into one artifact-layout package outside `domain/`, e.g. `executor-controller/service/artifacts` or `pkg/`, referenced by handler and adapter alike.

### [NIT] Temporal "now catches … rather than" wording in arch doc
- **Where:** `docs/arch/services/executor-controller.md:133`
- **Rule:** Layer 2 rule 5 — arch docs describe current state only.
- **Problem:** "Load-time validation **now** catches the two preconditions … as fatal boot errors **rather than** a silent full-parse discovered only after release" narrates a change relative to a prior state instead of stating the current behavior.
- **Suggested fix:** "Load-time validation catches the two preconditions a team dialect could otherwise violate silently — … — as fatal boot errors."

### [NIT] JSON tags on domain command struct extended
- **Where:** `executor-controller/domain/command/command.go:69-70` (`ParseProdS3URI`/`ParseCandidateS3URI` with `json:"...,omitempty"`)
- **Rule:** Layer 1 — domain purity (serialization details belong in adapters).
- **Problem:** The branch extends the pre-existing, already-recorded debt of serialization tags on `domain/command` structs (persisted verbatim as `job_params` JSONB). Consistent with the surrounding struct, so not a new class of violation — but the debt grows two fields.
- **Suggested fix:** Fold into the existing planned cleanup (adapter-side DTO for `job_params` marshalling); no standalone action needed on this branch.

### [NIT] Job `mode` label values inlined as string literals in the k8s-controller handler
- **Where:** `k8s-controller/service/handlers/check_status_handler.go:167` (`labels["mode"] == "compile"`, newly added) and `:654`; pre-existing `"validation"`/`"seed_build"` at `:159,163`
- **Rule:** Layer 1 — cross-service contract expressed as scattered literals.
- **Problem:** `promote_seed` has a shared constant (`pkg/events.ModePromoteSeed`, used at `:171`), while the other three mode values — stamped by executor-controller's adapter and mirrored in `executor-controller/domain/model/deployment.go:33-35` — are re-typed literals in the consumer. The branch extends the pattern with `"compile"` rather than introducing it.
- **Suggested fix:** Promote the three mode values next to `ModePromoteSeed` in `pkg/events` and use them from both services (follow-up-sized change).

## Clean

- **Dependency direction:** no new application→adapter imports anywhere in the diff. The only `service/ → adapters/` imports in touched services remain the pre-existing, untouched `executor-controller/service/uow/{uow,fake}.go`. All touched handler packages (`executor-controller/service/handlers`, `k8s-controller/service/handlers`, `release-controller/service/handlers`) import only domain, ports, uow, and `pkg/*`.
- **AST guard coverage:** every touched application dir is already listed in `handlerDirs` (`pkg/streams/handler_imports_test.go:21-33`), including `executor-controller/service/validation` and `executor-controller/service/deployer`; no new services/dirs were introduced that would need adding.
- **Port placement & assertions:** the `CommandResolver` extensions (`ParseCommand`, `PartialParsePath`) live on the existing port in `executor-controller/service/ports/command_resolver.go`; `adapters/commandcfg.Resolver` implements it with `var _ ports.CommandResolver = (*Resolver)(nil)` (`resolver.go:16`). No adapter declares a port consumed by the application layer (the k8s adapter consuming the service-owned `CommandResolver` is adapter-to-adapter wiring through an application-owned interface — permitted).
- **Domain purity (types):** new domain code (`domain/deploy/parse_cache.go`, `domain/events/compile_requested.go`, `domain/events/compile_node_completed.go`, `Deployment.failedContainer`, `k8s-controller/domain/model.K8sPodResult` additions) imports no Neo4j/Postgres/Redis/gRPC/K8s/S3 clients or framework types (only `uuid` as a value type). All K8s/S3/Redis/SQL specifics stay in `adapters/*` and the Python sidecar.
- **Stream/consumer-group constants:** zero inline versioned stream names or service-prefixed group names in any added Go, Python, or test line; every new outbox row uses `streams.CompileNodeCompletedV1`, `streams.CompileCompletedV1`, `streams.ReleaseRejectedV1`, `streams.ReleaseRequestedV1`, etc. New wire *fields* (`candidate_schema`, `failed_container`, `parse_cache`, `parse_cache_reason`) are payload keys, not stream names — fine.
- **Handler thinness:** `CompileRequestedHandler.Handle` maps event→command and enqueues via the repo; `CompileNodeCompletedHandler` delegates outcome recording to the `Deployment` aggregate (`RecordOutcome`); `HandleCompileResult` orchestrates transitions on the `Release` aggregate + outbox within the UoW, with the container→reason mapping isolated in the pure `compileRejection` function; `CheckStatusHandler` additions (`handleCompileTerminal`, `parseCacheFromResult`) mirror the established validation/seed-build structure and reach S3/K8s only through `ports.LogUploader` / the `K8sStatusChecker` seam.
- **Repository boundaries:** `failed_container` and `parse_cache(/‑reason)` columns are mapped entirely inside `adapters/postgres` row types; the domain aggregates are rebuilt via `Reconstitute*` constructors — no SQL or row types leak inward.
- **`Get*` vs `Load*`:** no new repository methods violate the convention (all touched `Get*` are plain reads; no mislabeled `Load*`).
- **Arch-doc currency (breadth):** all six touched services plus `02-interaction-matrix.md`, `03-sequence-flows.md`, and `05-error-classification.md` were updated in the same change. Spot-checks passed: parse-cache S3 URI layouts, rehearsal exit codes 42–46 and dbt log markers, `compile.completed:v1` payload (`candidate_schema`, per-node `failed_container`), `parse_cache` `varchar(16)` / `parse_cache_reason` `TEXT` vs migrations `V21`/`V30`, remediation's no-heal-evidence rule for `parse_rehearsal_failed`/`artifact_upload_failed` — all match the code. The two exceptions are the SHOULD-FIX/NIT doc findings above.
- **Migrations:** `db/migration/executor/V21__add_failed_container.sql` and `db/migration/state/V30__add_task_execution_parse_cache.sql` are additive, nullable, and comment-documented in current-state terms.

## Resolution (2026-07-19)

All findings below were fixed in one follow-up commit on this branch.

- **SF1 (arch-doc drift):** Fixed. `docs/arch/services/executor-controller.md:131` now states unknown operation keys are logged (`unknown_fields`) and dropped, not fatal, while a block left incomplete after dropping them still fails validation; the `:133` "now catches … rather than" temporal wording was rewritten to current-state phrasing.
- **SF2 (parse-cache contract literals):** Fixed. New `pkg/parsecache` package holds `ContainerName`/`Hydrated`/`DegradedPrefix`; `executor-controller/adapters/k8s/client.go` and `k8s-controller/service/handlers/check_status_handler.go` both consume it, and `pkg/parsecache/contract_test.go` guards the constants against `dbt/s3-sidecar/parse_cache_fetcher.py`'s literals, mirroring `pkg/validationresult`'s Python-binding guard.
- **SF3 (candidate-schema duplication):** Fixed. New `release-controller/service/handlers/candidate_schema.go` exports `CandidateSchemaFor(releaseID string) string`; all six inline `"_candidate_" + SanitizeSchemaSuffix(...)` call sites (`advance_queue.go`, `handle_parsed_manifest.go` ×2, `handle_validation_result.go`, `handle_seed_build_result.go` ×2) and the matching test assertion in `advance_queue_test.go` now call the helper.
- **NIT1 (URI builders' home):** Fixed. `ParseCacheProdURI`/`ParseCacheCandidateURI` moved to new `executor-controller/service/artifacts/parse_cache.go` (package `artifacts`), which also gained `ManifestURI(bucket, service, releaseID string) string`, now used by `compile_requested_handler.go` in place of its inline manifest-URI concatenation. All consumers (`adapters/k8s/client.go`, `service/handlers/compile_requested_handler.go`, and their tests) were repointed; `domain/deploy/parse_cache.go` and its test were deleted and the unit test moved to `service/artifacts/parse_cache_test.go`.
- **NIT2 (mode-label constants):** Fixed. `pkg/events` gained `ModeCompile`, `ModeValidation`, `ModeSeedBuild` beside `ModePromoteSeed`; `k8s-controller/service/handlers/check_status_handler.go`'s four mode-label comparisons (routing switch + running-announcement suppression) and `executor-controller/adapters/k8s/client.go`'s three mode-label stamping sites now use the shared constants instead of re-typed literals.
- **NIT3 (json-tags debt):** Resolved by documentation, consistent with the recorded repo-wide debt — no restructuring. `ValidationDeployTask`'s doc comment now states its fields carry json tags because the struct is marshalled verbatim into `job_params` JSONB, the same debt class as `DeployTask.Mode`.
