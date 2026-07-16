# Parse-Free dbt Workers Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Execute production dbt nodes in reusable pull-based worker pods with a release-pinned in-memory Manifest, while preserving exact dbt materialization behavior, custom command wrappers, the existing lifecycle streams, and one configurable global execution limit.

**Architecture:** Release compilation publishes `manifest.json`, `partial_parse.msgpack`, and a checksummed descriptor; release-controller and orchestrator pin the runtime reference through topology and run snapshots. Executor-controller routes production records to either legacy Jobs or pool-scoped worker leases, atomically shares capacity across both paths, and exposes an authenticated internal lease API. A thin Python runtime already present in each team dbt image hydrates one Manifest per pod and executes tasks sequentially through `dbtRunner` or the exact configured wrapper argv.

**Tech Stack:** Go 1.25, Python 3.12, dbt Core 1.12.0b1/dbt-postgres 1.10.0, PostgreSQL, Redis Streams/outbox, Neo4j, AWS SDK v2/S3-compatible storage, Kubernetes Deployments/Secrets/Jobs, Docker Compose, Helm, Kind.

---

## Fixed contracts

- Design source: `docs/superpowers/specs/2026-07-16-parse-free-dbt-workers-design.md`.
- The first rollout value is `EXECUTION_MODE=jobs`. A worker canary is enabled only with `EXECUTION_MODE_OVERRIDES_JSON`.
- `MAX_CONCURRENT_EXECUTIONS` is required and positive. Compose, Helm, and e2e set it to `50`; application logic contains no fallback literal `50`. `MAX_CONCURRENT_JOBS` is accepted only as a transition alias.
- Pool key input is exactly `service_name + "\x00" + image_tag + "\x00" + runtime_manifest_sha256`; the key is the lowercase SHA-256 hex digest.
- Graph `unique_id` remains `schema.table`. `dbt_unique_id` is a new, separate field containing dbt's key such as `model.service_1.table_a`.
- Runtime fields are additive and flat on node/task wires: `dbt_unique_id`, `runtime_manifest_uri`, `runtime_manifest_sha256`, `runtime_manifest_dbt_version`, and `runtime_manifest_parse_context_sha256`.
- A worker task invokes one exact `dbt_unique_id` and one exact resolved argv. It never derives SQL or reconstructs a materialization.
- Native argv (`filepath.Base(argv[0]) == "dbt"`) uses `dbtRunner(manifest=manifest).invoke(argv[1:])`. Other argv executes as an unchanged child process.
- `dbt-commands.yaml` remains authoritative. `compile.partial_parse_path` is optional; its default is `<dirname(manifest_path)>/partial_parse.msgpack`. Optional `worker.wrapper_cache` is `required` or `opaque`; native dbt ignores it.
- Worker credentials enter through a Secret-backed environment value, are popped into worker memory at startup, and are absent before dbt or a wrapper runs.
- There is no automatic full-project parse fallback. Missing fields on historical pre-migration messages select Jobs; a migrated node with `dbt_unique_id` but an incomplete runtime reference fails worker dispatch explicitly.
- Warehouse execution remains at-least-once.
- Every shell command in this plan is run through `rtk` and tests run in repository Docker images/containers.

## File structure

### Shared contracts and release artifacts

- Create `pkg/domain/model/runtime_manifest.go` and `runtime_manifest_test.go`: shared runtime reference, descriptor, validation, and annotation key.
- Modify `executor-controller/service/ports/command_resolver.go` and `adapters/commandcfg/{config,load,resolver}.go`: compile path, wrapper policy, and stable command-context serialization.
- Create `executor-controller/service/runtimecontext/context.go` and `context_test.go`: canonical controller-side parse context.
- Create `dbt/base/continuo_dbt_runtime/{descriptor,parse_context,export_artifacts}.py` and `dbt/base/bin/continuo-export-runtime-manifest`.
- Modify `dbt/base/Dockerfile`, `dbt/s3-sidecar/compile_uploader.py`, and compile Job construction/tests.

### Metadata propagation

- Modify `manifest-controller/domain/model.py`, `service/parser.py`, `adapters/sources/s3.py`, `service/candidate_manifest_handler.py`, `adapters/redis/candidate_publisher.py`, and `main.py`.
- Create `manifest-controller/service/runtime_manifest.py`.
- Create `db/migration/release/V13__add_runtime_manifest_to_service_prod.sql`.
- Modify release domain, repository, parse handler, promotion handler, and tests listed in Tasks 4-5.
- Modify orchestrator promotion, topology, snapshot, run, Neo4j, publisher, parser, and selector files listed in Task 5.

### Executor leases and shared capacity

- Create `db/migration/executor/V21__add_worker_leases_and_capacity.sql`.
- Create `executor-controller/domain/model/{execution_mode,lease}.go`, `domain/workerpool/pool.go`, and tests.
- Modify `domain/model/deployment.go` and repository/UoW/Postgres mapping.
- Create `executor-controller/service/lease/service.go`, `service/capacity/service.go`, `service/pool/reconciler.go`, `service/reaper/reaper.go`, and ports/tests.
- Create `executor-controller/adapters/http/{server,auth,worker_handler,dto}.go` and tests.
- Create `executor-controller/adapters/s3/presigner.go` and tests.
- Create `executor-controller/adapters/k8s/worker_pools.go` and tests.

### Worker runtime

- Create `dbt/base/continuo_dbt_runtime/{api_client,artifact_store,execution,worker}.py` and `dbt/base/bin/continuo-dbt-worker`.
- Create `dbt/tests/test_{runtime_artifact,worker_api_client,worker_execution,worker_loop}.py`.
- Modify `dbt/Dockerfile.upload` only as a test image so pytest can run against the base runtime; production team image dependencies remain unchanged.

### Job capacity, deployment, and verification

- Modify `pkg/streams/contract.yaml` and regenerate Go/Python bindings.
- Create `pkg/events/executor_job_terminal.go`; extend the durable Job event chain.
- Modify k8s-controller status handling/publishing and executor terminal-event consumption.
- Modify `docker-compose.yml`, Helm values/RBAC/templates, e2e Kubernetes manifests, and build scripts.
- Create `tests/e2e/worker_execution_test.go`, `worker_failure_test.go`, and `worker_performance_test.go` plus dbt fixtures.
- Reconcile all architecture pages named in Task 17.

---

## Slice 1 — Runtime artifact, propagation, and pinning

### Task 1: Add the shared runtime reference and command-context contract

**Files:**
- Create: `pkg/domain/model/runtime_manifest.go`
- Create: `pkg/domain/model/runtime_manifest_test.go`
- Modify: `pkg/domain/model/model.go`
- Modify: `executor-controller/service/ports/command_resolver.go`
- Modify: `executor-controller/adapters/commandcfg/config.go`
- Modify: `executor-controller/adapters/commandcfg/load.go`
- Modify: `executor-controller/adapters/commandcfg/resolver.go`
- Modify: `executor-controller/adapters/commandcfg/{load,resolver,deployed_config,dev_config}_test.go`
- Modify: `executor-controller/adapters/k8s/client.go`
- Modify: `executor-controller/adapters/k8s/{create_compile_job,command_dialect}_test.go`
- Create: `executor-controller/service/runtimecontext/context.go`
- Create: `executor-controller/service/runtimecontext/context_test.go`
- Modify: `config/dbt-commands.yaml`
- Modify: `deploy/app/files/dbt-commands.yaml`
- Modify: `tests/e2e/k8s/executor-controller-deployment.yaml`

- [ ] **Step 1: Write the shared-value tests**

Cover empty, partial, complete, descriptor conversion, exact lowercase-hex validation, and deterministic pool keys:

```go
func TestRuntimeManifestRefComplete(t *testing.T) {
    ref := RuntimeManifestRef{
        RuntimeManifestURI:                "s3://continuo/finance/r1/partial_parse.msgpack",
        RuntimeManifestSHA256:             strings.Repeat("a", 64),
        RuntimeManifestDBTVersion:          "1.12.0b1",
        RuntimeManifestParseContextSHA256: strings.Repeat("b", 64),
    }
    require.NoError(t, ref.Validate())
    assert.True(t, ref.Complete())
    assert.Equal(t,
        "2f05cf2ba42b4ecf8d92dc00a11a0c706e8164af241ef43c21fb2bd6c2e0814b",
        WorkerPoolKey("finance", "sha-123", ref.RuntimeManifestSHA256))
}
```

- [ ] **Step 2: Run the tests and verify the contract is absent**

Run:

```bash
rtk docker exec executor-controller go test ./../pkg/domain/model -run 'TestRuntimeManifest' -count=1
```

Expected: FAIL because `RuntimeManifestRef` and `WorkerPoolKey` do not exist.

- [ ] **Step 3: Implement the shared values and annotation**

```go
const (
    RuntimeManifestFormatV1          = "dbt-partial-parse-msgpack-v1"
    AnnotationExecutorDeploymentID  = "continuo.dev/executor-deployment-id"
)

type RuntimeManifestRef struct {
    RuntimeManifestURI                string `json:"runtime_manifest_uri,omitempty"`
    RuntimeManifestSHA256             string `json:"runtime_manifest_sha256,omitempty"`
    RuntimeManifestDBTVersion         string `json:"runtime_manifest_dbt_version,omitempty"`
    RuntimeManifestParseContextSHA256 string `json:"runtime_manifest_parse_context_sha256,omitempty"`
}

func (r RuntimeManifestRef) Complete() bool {
    return r.RuntimeManifestURI != "" &&
        r.RuntimeManifestSHA256 != "" &&
        r.RuntimeManifestDBTVersion != "" &&
        r.RuntimeManifestParseContextSHA256 != ""
}

func (r RuntimeManifestRef) Validate() error {
    if r == (RuntimeManifestRef{}) {
        return nil
    }
    if !r.Complete() {
        return fmt.Errorf("runtime manifest reference is partial")
    }
    if !strings.HasPrefix(r.RuntimeManifestURI, "s3://") {
        return fmt.Errorf("runtime_manifest_uri must be s3://")
    }
    for name, value := range map[string]string{
        "runtime_manifest_sha256": r.RuntimeManifestSHA256,
        "runtime_manifest_parse_context_sha256": r.RuntimeManifestParseContextSHA256,
    } {
        if len(value) != 64 || strings.ToLower(value) != value {
            return fmt.Errorf("%s must be lowercase SHA-256 hex", name)
        }
        if _, err := hex.DecodeString(value); err != nil {
            return fmt.Errorf("%s must be lowercase SHA-256 hex: %w", name, err)
        }
    }
    return nil
}

type RuntimeManifestDescriptor struct {
    Format             string `json:"format"`
    ServiceName        string `json:"service_name"`
    ReleaseID          string `json:"release_id"`
    ImageTag           string `json:"image_tag"`
    ArtifactURI        string `json:"artifact_uri"`
    SHA256             string `json:"sha256"`
    DBTCoreVersion     string `json:"dbt_core_version"`
    AdapterType        string `json:"adapter_type"`
    ParseContextSHA256 string `json:"parse_context_sha256"`
}

func WorkerPoolKey(serviceName, imageTag, manifestSHA string) string {
    sum := sha256.Sum256([]byte(serviceName + "\x00" + imageTag + "\x00" + manifestSHA))
    return hex.EncodeToString(sum[:])
}
```

- [ ] **Step 4: Extend command configuration with explicit compile and wrapper values**

Use these port types:

```go
type CompileCommand struct {
    Argv             []string
    ManifestPath     string
    PartialParsePath string
}

type WrapperCachePolicy string

const (
    WrapperCacheRequired WrapperCachePolicy = "required"
    WrapperCacheOpaque   WrapperCachePolicy = "opaque"
)

type CommandResolver interface {
    NodeCommand(serviceName string, op pkgmodel.Operation, nt pkgmodel.NodeType, node string) []string
    SeedBuildCommand(serviceName, node, targetSchema string) []string
    CompileCommand(serviceName string) CompileCommand
    WrapperCachePolicy(serviceName string) WrapperCachePolicy
    RuntimeContext(serviceName string) string
}
```

Add YAML fields:

```go
type compileSpec struct {
    Command          []string `yaml:"command"`
    ManifestPath     string   `yaml:"manifest_path"`
    PartialParsePath string   `yaml:"partial_parse_path,omitempty"`
}

type workerSpec struct {
    WrapperCache string `yaml:"wrapper_cache,omitempty"`
}

type opSet struct {
    Run       []string     `yaml:"run"`
    Seed      []string     `yaml:"seed"`
    Snapshot  []string     `yaml:"snapshot"`
    SeedBuild []string     `yaml:"seed_build"`
    Test      []string     `yaml:"test"`
    Build     []string     `yaml:"build"`
    Compile   *compileSpec `yaml:"compile"`
    Worker    *workerSpec  `yaml:"worker,omitempty"`
}
```

Resolver rules are exact:

```go
func (r *Resolver) CompileCommand(service string) ports.CompileCommand {
    spec := r.compileSpec(service)
    partial := spec.PartialParsePath
    if partial == "" {
        partial = filepath.Join(filepath.Dir(spec.ManifestPath), "partial_parse.msgpack")
    }
    return ports.CompileCommand{
        Argv: append([]string(nil), spec.Command...),
        ManifestPath: spec.ManifestPath,
        PartialParsePath: partial,
    }
}

func (r *Resolver) WrapperCachePolicy(service string) ports.WrapperCachePolicy {
    if ops := r.cfg.Services[service]; ops != nil && ops.Worker != nil {
        return ports.WrapperCachePolicy(ops.Worker.WrapperCache)
    }
    return ports.WrapperCacheOpaque
}
```

Update `CreateCompileJob` in this task to consume `ports.CompileCommand` while still passing `Argv` and `ManifestPath` to the current builder. Task 2 then adds the new partial-parse/context behavior. This keeps the Task 1 commit buildable.

`RuntimeContext` returns canonical JSON containing all seven raw resolved templates, compile paths, and wrapper policy. Sort map keys by marshaling a struct rather than a map. Validation accepts only `required` or `opaque` and requires absolute manifest/partial-parse paths.

- [ ] **Step 5: Implement the controller parse-context builder**

```go
type Context struct {
    CommandDialectSHA256 string            `json:"command_dialect_sha256"`
    Target               Target            `json:"target"`
    EnvironmentSHA256    map[string]string `json:"environment_sha256"`
}

type Target struct {
    Name     string `json:"name"`
    Type     string `json:"type"`
    Database string `json:"database"`
    Schema   string `json:"schema"`
}

func Build(commandContext string, getenv func(string) string) (string, error) {
    commandSum := sha256.Sum256([]byte(commandContext))
    keys := []string{"DBT_TARGET", "DBT_POSTGRES_DB", "DBT_TARGET_SCHEMA", "DBT_PROFILES_DIR", "DBT_PROJECT_DIR"}
    envHashes := make(map[string]string, len(keys))
    for _, key := range keys {
        valueSum := sha256.Sum256([]byte(getenv(key)))
        envHashes[key] = hex.EncodeToString(valueSum[:])
    }
    targetName := getenv("DBT_TARGET")
    if targetName == "" {
        targetName = "dev"
    }
    body, err := json.Marshal(Context{
        CommandDialectSHA256: hex.EncodeToString(commandSum[:]),
        Target: Target{
            Name: targetName, Type: "postgres",
            Database: getenv("DBT_POSTGRES_DB"),
            Schema: getenv("DBT_TARGET_SCHEMA"),
        },
        EnvironmentSHA256: envHashes,
    })
    return string(body), err
}
```

Tests must prove deterministic JSON, a changed command/env changes the JSON, and raw password values never appear.

- [ ] **Step 6: Update all three YAML configs**

Add `partial_parse_path` only where a wrapper moves it; standard/default configs omit it and exercise derivation. Add this to the deployed finance block and e2e service-1 block:

```yaml
worker:
  wrapper_cache: required
```

Keep the exact existing argv unchanged.

- [ ] **Step 7: Run focused tests**

```bash
rtk docker exec executor-controller go test ./../pkg/domain/model ./adapters/commandcfg ./adapters/k8s ./service/runtimecontext -count=1
```

Expected: PASS.

- [ ] **Step 8: Commit**

```bash
rtk git add pkg/domain/model executor-controller/service/ports/command_resolver.go executor-controller/service/runtimecontext executor-controller/adapters/commandcfg executor-controller/adapters/k8s/client.go executor-controller/adapters/k8s/create_compile_job_test.go executor-controller/adapters/k8s/command_dialect_test.go config/dbt-commands.yaml deploy/app/files/dbt-commands.yaml tests/e2e/k8s/executor-controller-deployment.yaml
rtk git commit -m "feat: define runtime manifest and command context"
```

### Task 2: Export and upload all release runtime artifacts

**Files:**
- Create: `dbt/base/continuo_dbt_runtime/__init__.py`
- Create: `dbt/base/continuo_dbt_runtime/descriptor.py`
- Create: `dbt/base/continuo_dbt_runtime/parse_context.py`
- Create: `dbt/base/continuo_dbt_runtime/export_artifacts.py`
- Create: `dbt/base/bin/continuo-export-runtime-manifest`
- Modify: `dbt/base/Dockerfile`
- Modify: `dbt/Dockerfile.upload`
- Modify: `dbt/s3-sidecar/compile_uploader.py`
- Modify: `dbt/tests/test_compile_uploader.py`
- Create: `dbt/tests/test_runtime_artifact.py`
- Modify: `executor-controller/adapters/k8s/client.go`
- Modify: `executor-controller/adapters/k8s/create_compile_job_test.go`
- Modify: `executor-controller/adapters/k8s/command_dialect_test.go`

- [ ] **Step 1: Write artifact tests**

Use a small fake Manifest object for hashing tests and a real `Manifest.to_msgpack()` round trip for hydration:

```python
def test_export_writes_three_bound_files(tmp_path, monkeypatch):
    manifest = make_manifest(service="finance", dbt_version="1.12.0b1")
    source = tmp_path / "partial_parse.msgpack"
    source.write_bytes(manifest.to_msgpack())
    manifest_json = tmp_path / "manifest.json"
    manifest_json.write_text('{"metadata": {"dbt_version": "1.12.0b1"}}')

    descriptor = export_runtime_artifacts(
        manifest_path=manifest_json,
        partial_parse_path=source,
        output_dir=tmp_path / "shared",
        service_name="finance",
        release_id="r1",
        image_tag="sha-1",
        artifact_uri="s3://continuo/finance/r1/partial_parse.msgpack",
        controller_context='{"command_dialect_sha256":"abc"}',
    )

    assert descriptor["format"] == "dbt-partial-parse-msgpack-v1"
    assert descriptor["sha256"] == sha256(source.read_bytes()).hexdigest()
    assert (tmp_path / "shared/runtime-manifest.json").exists()
```

Uploader tests cover: all three uploads, old-image manifest-only upload, descriptor-without-msgpack failure, and exact sibling S3 keys.

- [ ] **Step 2: Run tests and verify failure**

```bash
rtk docker compose build dbt-compile-and-load
rtk docker compose up -d dbt-compile-and-load
rtk docker exec dbt-compile-and-load python -m pytest -v tests/test_runtime_artifact.py tests/test_compile_uploader.py
```

Expected: FAIL because the runtime module and multi-artifact uploader do not exist.

- [ ] **Step 3: Implement one canonical parse-context function**

```python
def _file_hash(value) -> dict[str, str]:
    return {"name": value.name, "checksum": value.checksum}


def parse_context_sha256(manifest: Manifest, controller_context: str) -> str:
    controller = json.loads(controller_context)
    state = manifest.state_check
    payload = {
        "controller": controller,
        "state_check": {
            "vars_hash": _file_hash(state.vars_hash),
            "project_env_vars_hash": _file_hash(state.project_env_vars_hash),
            "profile_env_vars_hash": _file_hash(state.profile_env_vars_hash),
            "profile_hash": _file_hash(state.profile_hash),
            "project_hashes": {
                key: _file_hash(value)
                for key, value in sorted(state.project_hashes.items())
            },
        },
        "parse_env_sha256": {
            key: hashlib.sha256(os.environ.get(key, "").encode()).hexdigest()
            for key in sorted(manifest.env_vars)
        },
    }
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(canonical).hexdigest()
```

The same function is imported by the exporter and the worker. It stores hashes, never plaintext environment values.

- [ ] **Step 4: Implement export and descriptor validation**

```python
FORMAT = "dbt-partial-parse-msgpack-v1"


def export_runtime_artifacts(*, manifest_path: Path, partial_parse_path: Path,
                             output_dir: Path, service_name: str, release_id: str,
                             image_tag: str, artifact_uri: str,
                             controller_context: str) -> dict:
    if not manifest_path.is_file():
        raise RuntimeError(f"manifest missing: {manifest_path}")
    packed = partial_parse_path.read_bytes()
    manifest = Manifest.from_msgpack(packed)
    descriptor = {
        "format": FORMAT,
        "service_name": service_name,
        "release_id": release_id,
        "image_tag": image_tag,
        "artifact_uri": artifact_uri,
        "sha256": hashlib.sha256(packed).hexdigest(),
        "dbt_core_version": manifest.metadata.dbt_version,
        "adapter_type": manifest.metadata.adapter_type,
        "parse_context_sha256": parse_context_sha256(manifest, controller_context),
    }
    validate_descriptor(descriptor)
    output_dir.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(manifest_path, output_dir / "manifest.json")
    (output_dir / "partial_parse.msgpack").write_bytes(packed)
    (output_dir / "runtime-manifest.json").write_text(
        json.dumps(descriptor, sort_keys=True, separators=(",", ":")) + "\n"
    )
    return descriptor
```

The bin script is an argparse-only entry point exposing every named argument above and returning nonzero on any mismatch.

- [ ] **Step 5: Install scripts without changing batch startup**

Add to `dbt/base/Dockerfile`:

```dockerfile
COPY continuo_dbt_runtime/ /opt/continuo/continuo_dbt_runtime/
COPY bin/continuo-export-runtime-manifest /continuo/bin/continuo-export-runtime-manifest
ENV PYTHONPATH="/opt/continuo"
RUN chmod 0555 /continuo/bin/continuo-export-runtime-manifest
```

Keep `ENTRYPOINT ["/entrypoint.sh"]` unchanged. Add pytest only to `dbt/Dockerfile.upload`, which is a Continuo-owned test/support image:

```dockerfile
RUN pip install --no-cache-dir pytest
```

- [ ] **Step 6: Make the uploader atomic at the artifact-set level**

`compile_uploader.py` always uploads `manifest.json`. If both optional paths exist, validate descriptor SHA against msgpack, then upload msgpack and descriptor to sibling keys. If neither exists, log `runtime artifact unavailable; manifest-only compatibility upload`. If exactly one exists, exit nonzero.

```python
manifest_key = key
prefix = manifest_key.rsplit("/", 1)[0]
objects = [(manifest_path, manifest_key)]
if runtime_descriptor_path and partial_parse_path:
    descriptor = json.loads(Path(runtime_descriptor_path).read_text())
    packed = Path(partial_parse_path).read_bytes()
    if hashlib.sha256(packed).hexdigest() != descriptor["sha256"]:
        raise RuntimeError("partial_parse.msgpack SHA does not match descriptor")
    objects.extend([
        (partial_parse_path, f"{prefix}/partial_parse.msgpack"),
        (runtime_descriptor_path, f"{prefix}/runtime-manifest.json"),
    ])
for local_path, object_key in objects:
    s3.upload_file(local_path, bucket, object_key)
```

- [ ] **Step 7: Update compile Job construction**

`buildCompilePodSpec` receives `ports.CompileCommand` and a controller-context JSON string. The init command remains team-command-first and preserves exact argv:

```text
<exact compile argv> &&
cp <manifest_path> /shared/manifest.json &&
if [ -x /continuo/bin/continuo-export-runtime-manifest ]; then
  /continuo/bin/continuo-export-runtime-manifest
    --manifest <manifest_path>
    --partial-parse <partial_parse_path>
    --output-dir /shared
    --service-name <service>
    --release-id <release>
    --image-tag <image>
    --artifact-uri <sibling partial_parse URI>
    --controller-context "$CONTINUO_RUNTIME_CONTEXT_JSON";
else
  echo "runtime exporter absent; manifest-only compatibility release";
fi
```

Construct the shell through existing `shellJoin/shellQuote`. Pass `COMPILE_PARTIAL_PARSE_PATH=/shared/partial_parse.msgpack` and `COMPILE_RUNTIME_DESCRIPTOR_PATH=/shared/runtime-manifest.json` to the uploader. Tests assert custom `partial_parse_path`, exact wrapper compile argv, all env values, and the compatibility branch.

- [ ] **Step 8: Run focused tests**

```bash
rtk docker build -t dbt-base:latest dbt/base
rtk docker compose build dbt-compile-and-load
rtk docker compose up -d dbt-compile-and-load
rtk docker exec dbt-compile-and-load python -m pytest -v tests/test_runtime_artifact.py tests/test_compile_uploader.py
rtk docker exec executor-controller go test ./adapters/k8s -run 'TestCreateCompile|TestCompileCommand' -count=1
```

Expected: PASS and the base image still reports `/entrypoint.sh` as its entrypoint.

- [ ] **Step 9: Commit**

```bash
rtk git add dbt/base dbt/Dockerfile.upload dbt/s3-sidecar/compile_uploader.py dbt/tests executor-controller/adapters/k8s
rtk git commit -m "feat: publish dbt runtime manifest artifacts"
```

### Task 3: Read descriptors and preserve dbt unique IDs in manifest-controller

**Files:**
- Modify: `manifest-controller/domain/model.py`
- Modify: `manifest-controller/domain/exceptions.py`
- Modify: `manifest-controller/service/parser.py`
- Create: `manifest-controller/service/runtime_manifest.py`
- Modify: `manifest-controller/adapters/sources/__init__.py`
- Modify: `manifest-controller/adapters/sources/s3.py`
- Modify: `manifest-controller/service/candidate_manifest_handler.py`
- Modify: `manifest-controller/adapters/redis/candidate_publisher.py`
- Modify: `manifest-controller/main.py`
- Modify: `manifest-controller/tests/test_{model,parser,sources,candidate_manifest_handler,candidate_publisher,main_candidate_wiring}.py`
- Create: `manifest-controller/tests/fixtures/runtime-manifest.json`

- [ ] **Step 1: Write failing source/parser/publisher tests**

```python
def test_parser_preserves_dbt_unique_id(tmp_path):
    nodes = parse_manifest(write_manifest(tmp_path, key="model.service_1.table_a"), "v1")
    assert nodes[0].dbt_unique_id == "model.service_1.table_a"


def test_publish_ok_includes_one_runtime_ref_per_service(redis):
    publisher.publish_ok("r1", topology=[], runtime_manifests={
        "service-1": RuntimeManifestRef(
            uri="s3://continuo/service-1/r1/partial_parse.msgpack",
            sha256="a" * 64,
            dbt_version="1.12.0b1",
            parse_context_sha256="b" * 64,
        )
    })
    body = json.loads(redis.last_fields["payload"])
    assert body["runtime_manifests"]["service-1"]["runtime_manifest_sha256"] == "a" * 64
```

Also test missing descriptor → `None` for old releases, malformed descriptor → permanent `MalformedRuntimeManifest` publication, descriptor service mismatch, and descriptor path derivation.

- [ ] **Step 2: Run manifest-controller tests**

```bash
rtk docker exec manifest-controller uv run pytest -v tests/test_parser.py tests/test_sources.py tests/test_candidate_manifest_handler.py tests/test_candidate_publisher.py
```

Expected: FAIL on missing `dbt_unique_id`/`runtime_manifests`.

- [ ] **Step 3: Add Python boundary values and strict validation**

```python
@dataclass(frozen=True)
class RuntimeManifestRef:
    uri: str
    sha256: str
    dbt_version: str
    parse_context_sha256: str

    def to_wire(self) -> dict[str, str]:
        return {
            "runtime_manifest_uri": self.uri,
            "runtime_manifest_sha256": self.sha256,
            "runtime_manifest_dbt_version": self.dbt_version,
            "runtime_manifest_parse_context_sha256": self.parse_context_sha256,
        }


@dataclass
class ManifestFile:
    path: str
    version: str
    image_tag: str = ""
    declared_service: str = ""
    runtime_manifest: RuntimeManifestRef | None = None
```

`parse_descriptor` requires the v1 format, expected service, exact sibling artifact URI, exact 64-character lowercase hashes, nonempty dbt version, `adapter_type == "postgres"`, and returns `RuntimeManifestRef`.

Invalid JSON or validation raises `MalformedRuntimeManifestError`. Wrap `source.list_manifests()` in `CandidateManifestHandler`; catch only this error, publish `status=failed` with `error_class="MalformedRuntimeManifest"`, and return normally so the Redis message ACKs. S3 transport/authentication errors continue to escape for retry.

- [ ] **Step 4: Download the sibling descriptor without listing**

For manifest key `<prefix>/manifest.json` derive `<prefix>/runtime-manifest.json`. Catch only S3 `404/NoSuchKey/NotFound` as legacy absence; authentication/network failures remain retryable exceptions. Validate before returning `ManifestFile`.

```python
descriptor_key = f"{key.rsplit('/', 1)[0]}/runtime-manifest.json"
try:
    response = self._s3.get_object(Bucket=self._bucket, Key=descriptor_key)
except ClientError as exc:
    code = exc.response.get("Error", {}).get("Code")
    if code not in {"404", "NoSuchKey", "NotFound"}:
        raise
    runtime_ref = None
else:
    descriptor = json.loads(response["Body"].read())
    runtime_ref = parse_descriptor(
        descriptor,
        expected_service=declared_service,
        expected_artifact_uri=(
            f"s3://{self._bucket}/{key.rsplit('/', 1)[0]}/partial_parse.msgpack"
        ),
    )
```

- [ ] **Step 5: Publish dbt IDs and per-service refs**

In `parse_manifest` set `dbt_unique_id=node_id`. In the handler, reject two manifests for the same service with different refs. Add `"dbt_unique_id": node.dbt_unique_id` to every topology node and call:

```python
self._publisher.publish_ok(
    release_id=release_id,
    topology=topology,
    runtime_manifests={
        mf.declared_service: mf.runtime_manifest.to_wire()
        for mf in manifests if mf.runtime_manifest is not None
    },
)
```

The empty-topology path passes `runtime_manifests={}` explicitly.

- [ ] **Step 6: Run the full Python service tests**

```bash
rtk docker exec manifest-controller uv run pytest -v
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
rtk git add manifest-controller
rtk git commit -m "feat: propagate dbt runtime descriptors from manifests"
```

### Task 4: Persist runtime references through release-controller promotion

**Files:**
- Create: `db/migration/release/V13__add_runtime_manifest_to_service_prod.sql`
- Modify: `release-controller/domain/release/release.go`
- Modify: `release-controller/domain/release/service_prod.go`
- Modify: `release-controller/adapters/postgres/service_prod_repository.go`
- Modify: `release-controller/service/handlers/handle_parsed_manifest.go`
- Modify: `release-controller/service/handlers/handle_validation_result.go`
- Modify: `release-controller/adapters/redis/manifest_loaded_candidate_binding.go`
- Modify: `release-controller/domain/release/release_test.go`
- Modify: `release-controller/domain/release/service_prod_test.go`
- Modify: `release-controller/adapters/postgres/service_prod_repository_test.go`
- Modify: `release-controller/service/handlers/handle_parsed_manifest_test.go`
- Modify: `release-controller/service/handlers/handle_validation_result_test.go`
- Modify: `release-controller/integration_test/happy_path_test.go`

- [ ] **Step 1: Write failing migration/domain tests**

```go
func TestAttachRuntimeManifestsKeepsGraphAndDBTIDsSeparate(t *testing.T) {
    topo := release.Topology{{UniqueID: "public.orders", DBTUniqueID: "model.finance.orders", ServiceName: "finance"}}
    refs := map[string]pkgmodel.RuntimeManifestRef{"finance": completeRuntimeRef()}
    got, err := attachRuntimeManifests(topo, refs)
    require.NoError(t, err)
    assert.Equal(t, "public.orders", got[0].UniqueID)
    assert.Equal(t, "model.finance.orders", got[0].DBTUniqueID)
    assert.Equal(t, completeRuntimeRef(), got[0].RuntimeManifestRef)
}
```

Repository tests assert nullable legacy columns, complete round trips, and upsert replacement.

- [ ] **Step 2: Run focused release tests**

```bash
rtk docker exec release-controller go test ./domain/release ./adapters/postgres ./service/handlers -run 'RuntimeManifest|ServiceProd|ParsedManifest|Promot' -count=1
```

Expected: FAIL because the fields and V13 migration are absent.

- [ ] **Step 3: Add the migration**

```sql
ALTER TABLE service_prod
    ADD COLUMN runtime_manifest_uri TEXT NULL,
    ADD COLUMN runtime_manifest_sha256 TEXT NULL,
    ADD COLUMN runtime_manifest_dbt_version TEXT NULL,
    ADD COLUMN runtime_manifest_parse_context_sha256 TEXT NULL,
    ADD CONSTRAINT service_prod_runtime_manifest_all_or_none CHECK (
        (runtime_manifest_uri IS NULL
         AND runtime_manifest_sha256 IS NULL
         AND runtime_manifest_dbt_version IS NULL
         AND runtime_manifest_parse_context_sha256 IS NULL)
        OR
        (runtime_manifest_uri IS NOT NULL
         AND runtime_manifest_sha256 IS NOT NULL
         AND runtime_manifest_dbt_version IS NOT NULL
         AND runtime_manifest_parse_context_sha256 IS NOT NULL)
    );
```

- [ ] **Step 4: Extend release domain types**

```go
type Node struct {
    UniqueID          string   `json:"unique_id"`
    DBTUniqueID       string   `json:"dbt_unique_id,omitempty"`
    SchemaName        string   `json:"schema_name"`
    TableName         string   `json:"table_name"`
    ServiceName       string   `json:"service_name"`
    NodeType          string   `json:"node_type"`
    ContentHash       string   `json:"content_hash"`
    TestCount         int      `json:"test_count"`
    ImageTag          string   `json:"image_tag"`
    UpstreamUniqueIDs []string `json:"upstream_unique_ids"`
    Schedule          string   `json:"schedule"`
    OriginalFilePath  string   `json:"original_file_path"`
    CandidateSQLURI   string   `json:"candidate_sql_uri,omitempty"`
    pkgmodel.RuntimeManifestRef
}

type ServiceProd struct {
    serviceName     string
    releaseID       string
    manifestS3Key  string
    imageTag        string
    runtimeManifest pkgmodel.RuntimeManifestRef
    updatedAt       time.Time
}

func NewServiceProd(serviceName, releaseID, manifestS3Key, imageTag string,
    updatedAt time.Time) *ServiceProd {
    return NewServiceProdWithRuntime(
        serviceName, releaseID, manifestS3Key, imageTag,
        pkgmodel.RuntimeManifestRef{}, updatedAt,
    )
}

func NewServiceProdWithRuntime(serviceName, releaseID, manifestS3Key, imageTag string,
    runtimeManifest pkgmodel.RuntimeManifestRef, updatedAt time.Time) *ServiceProd {
    return &ServiceProd{
        serviceName: serviceName, releaseID: releaseID,
        manifestS3Key: manifestS3Key, imageTag: imageTag,
        runtimeManifest: runtimeManifest, updatedAt: updatedAt,
    }
}

func (s *ServiceProd) RuntimeManifest() pkgmodel.RuntimeManifestRef {
    return s.runtimeManifest
}
```

Update every constructor call explicitly with either a real ref or `pkgmodel.RuntimeManifestRef{}`.

- [ ] **Step 5: Attach refs at the parse boundary**

```go
type HandleParsedManifestInput struct {
    ReleaseID       string                                  `json:"release_id"`
    Status          string                                  `json:"status"`
    Topology        release.Topology                        `json:"topology,omitempty"`
    RuntimeManifests map[string]pkgmodel.RuntimeManifestRef `json:"runtime_manifests,omitempty"`
    ErrorClass      string                                  `json:"error_class,omitempty"`
    ErrorDetail     string                                  `json:"error_detail,omitempty"`
}
```

`attachRuntimeManifests` validates each ref, copies it only to nodes of the matching service, permits absent refs for legacy Jobs, and rejects an unreferenced service key. Call it before `joinImageTags`.

- [ ] **Step 6: Persist and promote the changed service's exact ref**

Derive the changed service ref from promoted topology, requiring all its nodes to agree. Pass it to `NewServiceProdWithRuntime`. Keep the existing `NewServiceProd` signature as the explicit legacy/seed helper. Extend `promotedNodeWire` with the five additive fields and copy each explicitly:

```go
type promotedNodeWire struct {
    UniqueID                          string   `json:"unique_id"`
    DBTUniqueID                       string   `json:"dbt_unique_id,omitempty"`
    SchemaName                        string   `json:"schema_name"`
    TableName                         string   `json:"table_name"`
    ServiceName                       string   `json:"service_name"`
    NodeType                          string   `json:"node_type"`
    ContentHash                       string   `json:"content_hash"`
    TestCount                         int      `json:"test_count"`
    ImageTag                          string   `json:"image_tag"`
    UpstreamUniqueIDs                 []string `json:"upstream_unique_ids"`
    Schedule                          string   `json:"schedule"`
    Changed                           bool     `json:"changed"`
    OriginalFilePath                  string   `json:"original_file_path"`
    RuntimeManifestURI                string   `json:"runtime_manifest_uri,omitempty"`
    RuntimeManifestSHA256             string   `json:"runtime_manifest_sha256,omitempty"`
    RuntimeManifestDBTVersion          string   `json:"runtime_manifest_dbt_version,omitempty"`
    RuntimeManifestParseContextSHA256 string   `json:"runtime_manifest_parse_context_sha256,omitempty"`
}
```

`WithoutCandidateSQLURI` must preserve all five fields. `service_prod` SELECT/INSERT/UPSERT scans all four runtime columns and reconstitutes through `NewServiceProdWithRuntime`.

- [ ] **Step 7: Prove unchanged service pointers remain exact**

Keep `release.requested:v1` manifest entries unchanged. Each unchanged service retains its old `manifest_s3_key`, so manifest-controller derives and validates that same release directory's descriptor. Add an incremental-release test asserting the resulting candidate nodes and persisted `service_prod` row both contain the unchanged service's old runtime SHA, never the changed service's SHA.

- [ ] **Step 8: Run release tests**

```bash
rtk docker compose build release-controller
rtk docker exec release-controller go test ./... -count=1
```

Expected: PASS.

- [ ] **Step 9: Commit**

```bash
rtk git add db/migration/release release-controller
rtk git commit -m "feat: pin runtime manifests in release promotion"
```

### Task 5: Pin runtime metadata through Neo4j snapshots and dispatch wires

**Files:**
- Modify: `orchestrator/domain/event/release_promoted.go`
- Modify: `orchestrator/domain/topology/node.go`
- Modify: `orchestrator/domain/model.go`
- Modify: `orchestrator/domain/run/events.go`
- Modify: `orchestrator/domain/snapshot/rows.go`
- Modify: `orchestrator/domain/snapshot/snapshot.go`
- Modify: `orchestrator/domain/snapshot/{latest_full_dag,single_node,source_pinned_dag,rebase_partition}.go`
- Modify: `orchestrator/service/handlers/release_promoted_handler.go`
- Modify: `orchestrator/service/handlers/{handle_scheduler_started,handle_node_completed,dispatch_derived_run}.go`
- Modify: `orchestrator/adapters/redis/release_promoted_parser.go`
- Modify: `orchestrator/adapters/neo4j/{release_promotion_repository,topology_reader,snapshot_writer,run_aggregate_repository}.go`
- Modify: `orchestrator/adapters/publisher/outbox_publisher.go`
- Modify: `orchestrator/domain/event/release_promoted_test.go`
- Modify: `orchestrator/domain/topology/release_promoted_node_test.go`
- Modify: `orchestrator/domain/snapshot/{latest_full_dag,single_node,source_pinned_dag,rebase_partition}_test.go`
- Modify: `orchestrator/domain/run/run_test.go`
- Modify: `orchestrator/service/handlers/{release_promoted_handler,handle_scheduler_started,handle_node_completed,dispatch_derived_run}_test.go`
- Modify: `orchestrator/adapters/redis/release_promoted_parser_test.go`
- Modify: `orchestrator/adapters/neo4j/{release_promotion_repository,topology_reader,snapshot_writer,run_aggregate_repository}_test.go`
- Modify: `orchestrator/adapters/publisher/outbox_publisher_test.go`

- [ ] **Step 1: Add failing propagation tests**

For each selector, seed a `LatestTableRow` or `SourceTaskRow` with `DBTUniqueID` and a complete ref, then assert the `TaskProjection` matches. Add a publisher test asserting all five Redis fields. Add a run-unblock test proving values come from `:EXECUTES` rather than current `:Table`.

```go
assert.Equal(t, "model.finance.orders", projection.DBTUniqueID)
assert.Equal(t, oldPinnedRef, projection.RuntimeManifestRef)
```

- [ ] **Step 2: Run focused tests**

```bash
rtk docker exec orchestrator go test ./domain/snapshot ./domain/run ./adapters/neo4j ./adapters/publisher ./service/handlers -run 'RuntimeManifest|ReleasePromoted|Snapshot|Unblocked' -count=1
```

Expected: FAIL because the fields do not propagate.

- [ ] **Step 3: Add the fields to every domain hop**

Embed `pkgmodel.RuntimeManifestRef` and add `DBTUniqueID string` to:

```text
event.ReleasePromotedNode
topology.ReleasePromotedTopologyNode
snapshot.LatestTableRow
snapshot.SourceTaskRow
snapshot.TaskProjection
run.NodeUnblocked
domain.TableNode
domain.NodeReadyForExecution
```

Keep JSON tags flat on wire structs. Conversion functions copy every field explicitly; never assign current topology values during rerun/rebase.

- [ ] **Step 4: Persist the fields on both Neo4j layers**

Promotion Cypher sets these `:Table` properties:

```cypher
t.dbt_unique_id = n.dbt_unique_id,
t.runtime_manifest_uri = n.runtime_manifest_uri,
t.runtime_manifest_sha256 = n.runtime_manifest_sha256,
t.runtime_manifest_dbt_version = n.runtime_manifest_dbt_version,
t.runtime_manifest_parse_context_sha256 = n.runtime_manifest_parse_context_sha256
```

Snapshot Cypher copies the same properties onto `[e:EXECUTES]`. `TopologyReader` reads them from `Table` for fresh runs and from `EXECUTES` for rerun/rebase. `run_aggregate_repository.go` reads them when constructing `NodeUnblocked`.

- [ ] **Step 5: Publish additive query.model fields**

```go
if evt.DBTUniqueID != "" {
    values["dbt_unique_id"] = evt.DBTUniqueID
}
if evt.RuntimeManifestURI != "" {
    values["runtime_manifest_uri"] = evt.RuntimeManifestURI
    values["runtime_manifest_sha256"] = evt.RuntimeManifestSHA256
    values["runtime_manifest_dbt_version"] = evt.RuntimeManifestDBTVersion
    values["runtime_manifest_parse_context_sha256"] = evt.RuntimeManifestParseContextSHA256
}
```

Old rows/events omit all new fields.

- [ ] **Step 6: Run the complete orchestrator suite**

```bash
rtk docker compose build orchestrator
rtk docker exec orchestrator go test ./... -count=1
```

Expected: PASS, including promotion-during-run and source-pinned selector tests.

- [ ] **Step 7: Update Slice 1 architecture docs**

Modify:

```text
docs/arch/01-topology.md
docs/arch/02-interaction-matrix.md
docs/arch/03-sequence-flows.md
docs/arch/services/manifest-controller.md
docs/arch/services/release-controller.md
docs/arch/services/orchestrator.md
```

Document the three-object S3 layout, descriptor validation, separate graph/dbt IDs, `service_prod` carry-forward, and `:Table` → `:EXECUTES` pinning.

- [ ] **Step 8: Commit**

```bash
rtk git add orchestrator docs/arch/01-topology.md docs/arch/02-interaction-matrix.md docs/arch/03-sequence-flows.md docs/arch/services/manifest-controller.md docs/arch/services/release-controller.md docs/arch/services/orchestrator.md
rtk git commit -m "feat: pin dbt runtime manifests in run snapshots"
```

---

## Slice 2 — Executor lease domain, capacity, and internal API

### Task 6: Add worker lease and capacity persistence

**Files:**
- Create: `db/migration/executor/V21__add_worker_leases_and_capacity.sql`
- Create: `executor-controller/domain/model/execution_mode.go`
- Create: `executor-controller/domain/model/execution_mode_test.go`
- Create: `executor-controller/domain/model/lease.go`
- Create: `executor-controller/domain/model/lease_test.go`
- Modify: `executor-controller/domain/model/deployment.go`
- Modify: `executor-controller/domain/model/deployment_test.go`
- Modify: `executor-controller/domain/repository/port.go`
- Create: `executor-controller/domain/repository/cancelled_schedules.go`
- Modify: `executor-controller/adapters/postgres/cancelled_schedules_repository.go`
- Modify: `executor-controller/adapters/postgres/deployments_repository.go`
- Modify: `executor-controller/adapters/postgres/deployments_repository_test.go`
- Create: `executor-controller/adapters/postgres/capacity_repository_integration_test.go`
- Create: `executor-controller/adapters/postgres/unit_of_work.go`
- Create: `executor-controller/adapters/postgres/unit_of_work_wedge_test.go`
- Modify: `executor-controller/service/uow/uow.go`
- Modify: `executor-controller/service/uow/fake.go`
- Delete: `executor-controller/service/uow/uow_wedge_test.go`
- Modify: `executor-controller/main.go`
- Modify: `executor-controller/adapters/redis/{query_model,retry_task,validation_node_completed,validation_requested}_binding_integration_test.go`
- Modify: `executor-controller/service/deployer/validation_pipeline_integration_test.go`

- [ ] **Step 1: Write aggregate transition tests**

```go
func TestWorkerLeaseLifecycleAndStaleTokenFence(t *testing.T) {
    dep := NewWorkerDeployment(commandFixture(), uuid.Nil, poolFixture(), time.Unix(10, 0))
    token := strings.Repeat("1", 64)
    leaseID := uuid.New()

    require.NoError(t, dep.Claim(leaseID, sha256Hex(token), "worker-1", "pod-a", "uid-a",
        time.Unix(20, 0), time.Unix(80, 0), []string{"dbt", "run", "--select", "orders"},
        ExecutionPathNative))
    require.NoError(t, dep.AcknowledgeStart(leaseID, sha256Hex(token), time.Unix(21, 0)))
    assert.ErrorIs(t,
        dep.Complete(leaseID, sha256Hex("stale"), WorkerResult{Succeeded: true}, time.Unix(30, 0)),
        ErrStaleLease)
    require.NoError(t,
        dep.Complete(leaseID, sha256Hex(token), WorkerResult{Succeeded: true}, time.Unix(30, 0)))
    assert.Equal(t, StatusSucceeded, dep.Status())
    assert.NotNil(t, dep.SlotReleasedAt())
}
```

Also cover duplicate start/completion, claim only from due pending, heartbeat extension, retry-pending, cancellation, expiry, job reservation, and invalid transitions.

- [ ] **Step 2: Run domain tests**

```bash
rtk docker exec executor-controller go test ./domain/model -run 'Worker|Lease|Reservation|ExecutionMode' -count=1
```

Expected: FAIL because the lifecycle does not exist.

- [ ] **Step 3: Add the executor migration**

```sql
ALTER TABLE executor_deployments
    DROP CONSTRAINT executor_deployments_status_check,
    ADD COLUMN execution_mode TEXT NOT NULL DEFAULT 'jobs'
        CHECK (execution_mode IN ('jobs', 'workers')),
    ADD COLUMN pool_key TEXT NULL,
    ADD COLUMN resolved_argv JSONB NULL,
    ADD COLUMN execution_path TEXT NULL
        CHECK (execution_path IN ('native', 'wrapper_required', 'wrapper_opaque') OR execution_path IS NULL),
    ADD COLUMN slot_reserved_at TIMESTAMPTZ NULL,
    ADD COLUMN slot_released_at TIMESTAMPTZ NULL,
    ADD COLUMN lease_id UUID NULL,
    ADD COLUMN lease_token_sha256 TEXT NULL,
    ADD COLUMN lease_owner TEXT NULL,
    ADD COLUMN lease_pod_name TEXT NULL,
    ADD COLUMN lease_pod_uid TEXT NULL,
    ADD COLUMN attempt INT NOT NULL DEFAULT 0,
    ADD COLUMN lease_expires_at TIMESTAMPTZ NULL,
    ADD COLUMN heartbeat_at TIMESTAMPTZ NULL,
    ADD COLUMN started_at TIMESTAMPTZ NULL,
    ADD COLUMN finished_at TIMESTAMPTZ NULL,
    ADD COLUMN terminal_result JSONB NULL,
    ADD CONSTRAINT executor_deployments_status_check CHECK (
        status IN (
            'pending', 'blocked', 'dispatching', 'deployed', 'leased', 'running',
            'retry_pending', 'succeeded', 'failed', 'skipped', 'cancelled'
        )
    ),
    ADD CONSTRAINT executor_deployments_worker_pool_check CHECK (
        execution_mode <> 'workers' OR pool_key IS NOT NULL
    ),
    ADD CONSTRAINT executor_deployments_slot_order_check CHECK (
        slot_released_at IS NULL OR slot_reserved_at IS NOT NULL
    );

CREATE INDEX idx_executor_worker_due
    ON executor_deployments (pool_key, next_attempt_at, created_at)
    WHERE execution_mode = 'workers' AND status = 'pending';

CREATE UNIQUE INDEX uq_executor_active_lease_id
    ON executor_deployments (lease_id)
    WHERE lease_id IS NOT NULL;

CREATE TABLE executor_worker_pools (
    pool_key TEXT PRIMARY KEY,
    service_name TEXT NOT NULL,
    image_tag TEXT NOT NULL,
    runtime_manifest_uri TEXT NOT NULL,
    runtime_manifest_sha256 TEXT NOT NULL,
    runtime_manifest_dbt_version TEXT NOT NULL,
    runtime_manifest_parse_context_sha256 TEXT NOT NULL,
    credential_sha256 TEXT NOT NULL,
    desired_replicas INT NOT NULL DEFAULT 0 CHECK (desired_replicas >= 0),
    last_activity_at TIMESTAMPTZ NOT NULL,
    initialization_error TEXT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);
```

Update the existing due index predicate only if PostgreSQL requires recreation after the new statuses.

- [ ] **Step 4: Define modes, paths, lease identity, and result**

```go
type ExecutionMode string
const (
    ExecutionModeJobs    ExecutionMode = "jobs"
    ExecutionModeWorkers ExecutionMode = "workers"
)

type ExecutionPath string
const (
    ExecutionPathNative          ExecutionPath = "native"
    ExecutionPathWrapperRequired ExecutionPath = "wrapper_required"
    ExecutionPathWrapperOpaque   ExecutionPath = "wrapper_opaque"
)

type Reservation struct {
    ReservedAt *time.Time
    ReleasedAt *time.Time
}

type Lease struct {
    ID          uuid.UUID
    TokenSHA256 string
    Owner       string
    PodName     string
    PodUID      string
    Attempt     int
    ExpiresAt   time.Time
    HeartbeatAt time.Time
    StartedAt   *time.Time
    FinishedAt  *time.Time
}

type ActiveLease struct {
    DeploymentID uuid.UUID
    LeaseID      uuid.UUID
    PodName      string
    PodUID       string
}

type PoolDemand struct {
    PoolKey        string
    ServiceName    string
    ImageTag       string
    RuntimeManifest pkgmodel.RuntimeManifestRef
    Pending        int
    ActiveLeases   int
    OldestReadyAt  time.Time
}

type WorkerResult struct {
    Succeeded       bool    `json:"succeeded"`
    Retryable       bool    `json:"retryable"`
    ErrorClass      string  `json:"error_class,omitempty"`
    ErrorMessage    string  `json:"error_message,omitempty"`
    ExecutionSeconds float64 `json:"execution_seconds"`
    ReadyToDBTStartSeconds float64 `json:"ready_to_dbt_start_seconds,omitempty"`
    UploadSeconds          float64 `json:"upload_seconds,omitempty"`
    CacheStatus            string  `json:"cache_status,omitempty"`
    LogS3URI        string  `json:"log_s3_uri,omitempty"`
    RunResultsS3URI string  `json:"run_results_s3_uri,omitempty"`
    UnsafeRuntime   bool    `json:"unsafe_runtime,omitempty"`
}

var ErrStaleLease = errors.New("stale lease")
```

`Lease` stores only the SHA-256 of the raw token. Domain methods compare the provided hash with `subtle.ConstantTimeCompare` and return idempotent success only for the same lease/terminal result.

- [ ] **Step 5: Refactor Deployment reconstitution**

Replace growing positional reconstitution signatures with:

```go
type ReconstituteInput struct {
    ID                  uuid.UUID
    MessageProcessingID *uuid.UUID
    Mode                Mode
    Command             command.DeployTask
    ValidationCommand   command.ValidationDeployTask
    Status              Status
    RetryCount          int
    MaxRetries          int
    NextAttemptAt       time.Time
    CreatedAt           time.Time
    DeployedAt          *time.Time
    ErrorMessage        *string
    ExecutionMode       ExecutionMode
    PoolKey             string
    ResolvedArgv        []string
    ExecutionPath       ExecutionPath
    Reservation         Reservation
    Lease               *Lease
    Outcome             string
    DBTLogURI           string
    DBTRunResultsURI    string
    OutcomeAt           *time.Time
}
```

Keep `NewDeployment` as the Jobs-compatible constructor and add `NewWorkerDeployment`. Add statuses `dispatching`, `leased`, `running`, `retry_pending`, `succeeded`, and `cancelled`.

- [ ] **Step 6: Extend repository ports with atomic operations**

```go
type DeploymentRepository interface {
    Add(ctx context.Context, d *model.Deployment) error
    Save(ctx context.Context, d *model.Deployment) error
    GetByID(ctx context.Context, id uuid.UUID) (*model.Deployment, error)
    GetDueJobs(ctx context.Context, limit int) ([]*model.Deployment, error)
    GetDueWorkerForPool(ctx context.Context, poolKey string) (*model.Deployment, error)
    GetByReleaseNode(ctx context.Context, releaseID, nodeID string, mode model.Mode) (*model.Deployment, error)
    PendingValidationCount(ctx context.Context, releaseID string, mode model.Mode) (int, error)
    ListValidationResults(ctx context.Context, releaseID string, mode model.Mode) ([]*model.Deployment, error)
    ListValidationByRelease(ctx context.Context, releaseID string, mode model.Mode) ([]*model.Deployment, error)
    LockCapacity(ctx context.Context) error
    ActiveSlotCount(ctx context.Context) (int, error)
    ReleaseSlot(ctx context.Context, id uuid.UUID, now time.Time) (bool, error)
    GetExpiredLeaseForUpdate(ctx context.Context, now time.Time) (*model.Deployment, error)
    GetStaleDispatchingForUpdate(ctx context.Context, before time.Time) (*model.Deployment, error)
    ListPoolDemand(ctx context.Context, now time.Time) ([]model.PoolDemand, error)
    DemotePendingPoolToJobs(ctx context.Context, poolKey string, now time.Time) (int64, error)
    CancelSchedule(ctx context.Context, scheduleID uuid.UUID, now time.Time) ([]model.ActiveLease, error)
}
```

The Postgres adapter exposes one transaction-scoped advisory lock for the Jobs dispatcher and lease service:

```sql
SELECT pg_advisory_xact_lock(2147483001);
SELECT COUNT(*)
FROM executor_deployments
WHERE slot_reserved_at IS NOT NULL AND slot_released_at IS NULL;
```

If the count is below `limit`, select exactly one row with `FOR UPDATE SKIP LOCKED`. The lease application service generates the raw token with `crypto/rand`, mutates the aggregate, persists only its SHA, commits, and returns the raw token once.

The repository performs only persistence. `deployer.Dispatcher` and `lease.Service` own the transaction: lock capacity, count slots, select a due aggregate, resolve command/generate token in the application layer, mutate the aggregate, save, and commit.

- [ ] **Step 7: Move the concrete UnitOfWork to the Postgres adapter**

Keep only the `UnitOfWork` interface in `service/uow/uow.go`. Move `PostgresUnitOfWork` and its repository construction to `adapters/postgres/unit_of_work.go` with:

```go
var _ uow.UnitOfWork = (*UnitOfWork)(nil)

func NewUnitOfWork(db *sqlx.DB, logger *slog.Logger) *UnitOfWork {
    return &UnitOfWork{db: db, logger: logger}
}
```

Application code under `service` must import no adapter package.

Change main and integration-test factories to `postgres.NewUnitOfWork`. Move the wedge test to `adapters/postgres` so it tests the concrete adapter at its owning layer.

Move `CancelledSchedulesRepository` into `domain/repository/cancelled_schedules.go` in the same commit and add `var _ repository.CancelledSchedulesRepository = (*cancelledSchedulesRepository)(nil)` in the adapter; this lets the UoW interface return only inward-owned ports.

- [ ] **Step 8: Prove concurrency against real Postgres**

Start `MAX+10` application-service calls claiming across two pools and reserving Jobs. Assert:

```go
assert.LessOrEqual(t, maxObservedActiveSlots, limit)
assert.Len(t, uniqueDeploymentIDs, limit)
assert.Len(t, uniqueLeaseIDs, workerClaims)
```

Run:

```bash
rtk docker exec executor-controller go test ./adapters/postgres -run 'TestConcurrentCapacity|TestWorkerLeaseRepository' -count=1
```

Expected: PASS with no duplicate deployment and no cap overflow.

- [ ] **Step 9: Run executor tests and commit**

```bash
rtk docker exec executor-controller go test ./domain/model ./adapters/postgres ./service/uow -count=1
rtk git add db/migration/executor executor-controller/domain executor-controller/adapters/postgres executor-controller/service/uow
rtk git commit -m "feat: add durable worker leases and execution slots"
```

### Task 7: Route production records and pin exact commands

**Files:**
- Modify: `executor-controller/config/config.go`
- Modify: `executor-controller/config/config_test.go`
- Modify: `executor-controller/domain/events/query_model.go`
- Modify: `executor-controller/domain/events/retry_task.go`
- Modify: `executor-controller/domain/command/command.go`
- Modify: `executor-controller/domain/command/command_test.go`
- Modify: `executor-controller/adapters/redis/query_model_parser.go`
- Modify: `executor-controller/adapters/redis/query_model_parser_test.go`
- Modify: `executor-controller/adapters/redis/retry_task_parser.go`
- Modify: `executor-controller/adapters/redis/retry_task_parser_test.go`
- Create: `executor-controller/service/routing/mode.go`
- Create: `executor-controller/service/routing/mode_test.go`
- Create: `executor-controller/service/tasklifecycle/fanout.go`
- Create: `executor-controller/service/tasklifecycle/fanout_test.go`
- Modify: `executor-controller/service/handlers/create_deployment.go`
- Modify: `executor-controller/service/handlers/query_model_handler.go`
- Modify: `executor-controller/service/handlers/retry_task_handler.go`
- Modify: `executor-controller/service/handlers/query_model_handler_test.go`
- Modify: `executor-controller/service/handlers/retry_task_handler_test.go`
- Modify: `executor-controller/service/handlers/stubs_test.go`
- Modify: `executor-controller/main.go`

- [ ] **Step 1: Write config and routing tests**

```go
func TestModePolicy(t *testing.T) {
    policy := routing.NewPolicy(model.ExecutionModeWorkers,
        map[string]model.ExecutionMode{"legacy": model.ExecutionModeJobs})

    assert.Equal(t, model.ExecutionModeJobs,
        policy.Resolve("finance", "", "", pkgmodel.RuntimeManifestRef{})) // historical
    assert.Equal(t, model.ExecutionModeJobs,
        policy.Resolve("legacy", "", "model.legacy.orders", completeRef()))
    assert.Equal(t, model.ExecutionModeJobs,
        policy.Resolve("finance", pkgevents.ModePromoteSeed,
            "seed.finance.currency", completeRef()))
    assert.Equal(t, model.ExecutionModeWorkers,
        policy.Resolve("finance", "", "model.finance.orders", completeRef()))
    assert.Error(t,
        policy.Validate("finance", "", "model.finance.orders", pkgmodel.RuntimeManifestRef{}))
}
```

Config tests cover invalid JSON, unknown mode, nonpositive capacity, heartbeat not fitting at least three times in TTL, and alias precedence.

- [ ] **Step 2: Run tests and verify failure**

```bash
rtk docker exec executor-controller go test ./config ./service/routing ./adapters/redis ./service/handlers -run 'ExecutionMode|RuntimeManifest|QueryModel|RetryTask' -count=1
```

Expected: FAIL on missing configuration and event fields.

- [ ] **Step 3: Replace the concurrency config**

Add these fields verbatim to the current `Config`:

```go
ExecutionMode             model.ExecutionMode
ExecutionModeOverrides    map[string]model.ExecutionMode
MaxConcurrentExecutions   int
WorkerIdleTimeout         time.Duration
WorkerLeaseTTL            time.Duration
WorkerHeartbeatInterval   time.Duration
WorkerClaimWait           time.Duration
WorkerControlPlaneURL     string
```

Loading rules:

```go
rawLimit := os.Getenv("MAX_CONCURRENT_EXECUTIONS")
if rawLimit == "" {
    rawLimit = os.Getenv("MAX_CONCURRENT_JOBS")
}
if rawLimit == "" {
    v.Add("MAX_CONCURRENT_EXECUTIONS")
}
limit, err := strconv.Atoi(rawLimit)
if err != nil || limit <= 0 {
    v.Add("MAX_CONCURRENT_EXECUTIONS(positive)")
}
```

Use defaults `jobs`, idle `300s`, lease TTL `60s`, heartbeat `15s`, and claim wait `20s`. Validate `3 * heartbeat < lease TTL`. Remove `MaxConcurrentJobs`.

- [ ] **Step 4: Parse and persist all runtime fields**

Add `DBTUniqueID` plus embedded `pkgmodel.RuntimeManifestRef` to `QueryModel`, `RetryTask`, and `DeployTask`. Parsers accept all fields absent for old messages, validate the all-or-none ref, and reject a malformed partial ref.

Add these members verbatim to the current `DeployTask`:

```go
DBTUniqueID string `json:"dbt_unique_id,omitempty"`
pkgmodel.RuntimeManifestRef
ExecutorDeploymentID string `json:"executor_deployment_id,omitempty"`
```

- [ ] **Step 5: Implement deterministic routing**

`Policy.Validate` distinguishes:

```text
dbt_unique_id empty                              -> historical -> Jobs
dispatch mode promote_seed                       -> Jobs
service override jobs                            -> Jobs
effective workers + complete runtime reference  -> Workers
effective workers + dbt_unique_id present + incomplete reference -> permanent error
```

Compile, validation, and seed-build records already use separate constructors and always remain Jobs. Worker deployment creation applies only to ordinary production mode, sets `pool_key=pkgmodel.WorkerPoolKey(service,image,sha)`, and does not resolve argv yet.

If a migrated node is configured for workers but has an incomplete reference, create an audit deployment, call `RejectBeforeExecution("runtime manifest reference is incomplete")`, and write `task.status.updated:v1 FAILED` plus `node.updated:v1 FAILED` through `tasklifecycle.Fanout.DispatchRejected` in the same query-handler transaction. Return nil so the binding commits and ACKs; downstream run state must not hang on a dropped permanent message.

```go
func (Fanout) DispatchRejected(ctx context.Context, repo outbox.Repository,
    dep *model.Deployment, reason string) error
```

- [ ] **Step 6: Pin command once at claim**

Add this pure resolver used by `lease.Service` after it locks/selects a due deployment:

```go
func ResolveExecution(commands ports.CommandResolver, dep *model.Deployment) ([]string, model.ExecutionPath, error) {
    cmd := dep.Command()
    argv := commands.NodeCommand(cmd.ServiceName, pkgmodel.Operation(cmd.Operation),
        pkgmodel.NodeType(cmd.NodeType), cmd.TableName)
    if len(argv) == 0 || argv[0] == "" {
        return nil, "", fmt.Errorf("%w: empty resolved argv", pkgevents.ErrPermanent)
    }
    if filepath.Base(argv[0]) == "dbt" {
        return argv, model.ExecutionPathNative, nil
    }
    if commands.WrapperCachePolicy(cmd.ServiceName) == ports.WrapperCacheRequired {
        return argv, model.ExecutionPathWrapperRequired, nil
    }
    return argv, model.ExecutionPathWrapperOpaque, nil
}
```

The repository persists argv/path only when currently NULL and returns the persisted values on retry, so config reload cannot alter an attempted task.

- [ ] **Step 7: Preserve same-row retries for workers**

Add `executor_deployment_id` to retry parsing. When present, `RetryTaskHandler` loads that row, verifies `StatusRetryPending`, updates command retry count/job name, and calls `Requeue(now)`. When absent, keep existing Job behavior and add a new deployment.

- [ ] **Step 8: Run tests and commit**

```bash
rtk docker exec executor-controller go test ./config ./domain/... ./adapters/redis ./service/routing ./service/handlers -count=1
rtk git add executor-controller
rtk git commit -m "feat: route pinned dbt tasks to worker pools"
```

### Task 8: Share capacity with every Kubernetes Job

**Files:**
- Modify: `pkg/streams/contract.yaml`
- Regenerate: `pkg/streams/streams.gen.go`
- Regenerate: `manifest-controller/streams_contract.py`
- Create: `pkg/events/executor_job_terminal.go`
- Modify: `pkg/events/k8s_streams.go`
- Modify: `executor-controller/domain/deploy/ports.go`
- Modify: `executor-controller/domain/event/event.go`
- Modify: `executor-controller/service/deployer/dispatcher.go`
- Modify: `executor-controller/service/deployer/dispatcher_test.go`
- Modify: `executor-controller/adapters/k8s/{client,deployer}.go` and tests
- Create: `executor-controller/adapters/redis/job_terminal_parser.go`
- Create: `executor-controller/adapters/redis/job_terminal_parser_test.go`
- Create: `executor-controller/adapters/redis/job_terminal_binding.go`
- Create: `executor-controller/service/handlers/job_terminal_handler.go`
- Create: `executor-controller/service/handlers/job_terminal_handler_test.go`
- Modify: `executor-controller/main.go`
- Modify: `k8s-controller/domain/command/command.go`
- Modify: `k8s-controller/domain/event/event.go`
- Modify: `k8s-controller/service/handlers/check_status_handler.go`
- Modify: `k8s-controller/service/handlers/check_status_handler_test.go`
- Modify: `k8s-controller/adapters/publisher/outbox_publisher.go` and tests
- Modify: `k8s-controller/adapters/redis/{node_deployed_binding,check_job_status_parser}.go` and tests

- [ ] **Step 1: Add the stream contract and typed event**

Add to `contract.yaml`:

```yaml
  - name: executor.job.terminal:v1
    const: ExecutorJobTerminalV1
    description: Capacity-only terminal notification for executor-created Kubernetes Jobs.
    producers: [k8s-controller]
    consumers:
      - service: executor-controller
        group: executor-job-terminal
        const: ExecutorJobTerminal
```

```go
type ExecutorJobTerminal struct {
    ExecutorDeploymentID string `json:"executor_deployment_id"`
    JobName               string `json:"job_name"`
    TerminalStatus        string `json:"terminal_status"`
    CompletedAt           string `json:"completed_at"`
}
```

- [ ] **Step 2: Regenerate and verify bindings**

```bash
rtk docker run --rm -v /Users/simonecarolini/github/continuo:/src -w /src golang:1.25.1 go generate ./pkg/streams/...
rtk git diff --exit-code pkg/streams/contract.yaml pkg/streams/streams.gen.go manifest-controller/streams_contract.py
```

Expected: the second command exits nonzero before staging because generated files changed; inspect that only the new constants were added.

- [ ] **Step 3: Extend the durable Job check chain**

Add `ExecutorDeploymentID` and `Mode` to `pkg/events.NodeDeployed` and `CheckK8s`, executor `event.JobDeployed`, k8s `JobCheckRequest`, parsers, and self-loop publishing. Add the four runtime-reference fields as well so `retry.task:v1` can reproduce an old release exactly.

Every `deploy.JobSpec`/`ValidationJobSpec` carries `ExecutorDeploymentID`. All Job builders stamp:

```go
annotations[pkgmodel.AnnotationExecutorDeploymentID] = spec.ExecutorDeploymentID
```

`deploy.JobSpec` also carries optional `ResolvedArgv []string`. The K8s adapter uses it verbatim when present and otherwise calls the existing command resolver. This preserves command pinning when a pending/retry-pending worker record is rolled back to Jobs.

- [ ] **Step 4: Replace live Kubernetes counting with durable reservations**

Remove `CountActive` from `deploy.Deployer` and the K8s adapter. Dispatcher flow becomes:

```text
transaction A: lock capacity -> reserve one due Jobs-mode deployment -> status dispatching -> commit
Kubernetes: create idempotent Job outside transaction
transaction B success: status deployed + node.deployed outbox -> commit
transaction B failure: release slot + existing retry/failure transition -> commit
```

`DispatcherConfig.BatchSize == 0` resolves to `maxConcurrent`, not `50`. Before reserving new work, each dispatcher tick selects one stale `dispatching` reservation, keeps its slot reserved, repeats the idempotent Job create, and finalizes the deployed/failure transaction. This closes the create/commit crash window without temporarily undercounting an already-running Job.

- [ ] **Step 5: Observe promote-seed Jobs too**

Executor now emits `node.deployed:v1` for `promote_seed` solely to start status observation; it still emits no state-bound RUNNING/terminal business events. K8s carries `Mode` in the durable check message and routes it without depending on Job metadata.

- [ ] **Step 6: Emit one capacity event on every terminal branch**

At the point `CheckStatusHandler` establishes a terminal result, write:

```go
if cmd.ExecutorDeploymentID != "" {
    terminal := pkgevents.ExecutorJobTerminal{
        ExecutorDeploymentID: cmd.ExecutorDeploymentID,
        JobName: cmd.JobName,
        TerminalStatus: string(result.Status),
        CompletedAt: result.CompletedAt.UTC().Format(time.RFC3339Nano),
    }
    // marshal into k8s_outbox with streams.ExecutorJobTerminalV1
}
```

Call it before routing production/validation/seed-build/compile/promote-seed behavior. Duplicate terminal observations use the same deterministic aggregate/event identity and remain idempotent.

- [ ] **Step 7: Consume and release idempotently**

The executor parser validates UUID deployment ID and terminal status. Handler transaction calls `ReleaseSlot`; `false,nil` means already released and still ACKs. It never writes task/node lifecycle events.

Wire `streams.ExecutorJobTerminalV1`/`streams.ExecutorJobTerminal` in `main.go` in this task and start it with the existing consumers, so default Jobs mode releases capacity immediately at this checkpoint.

- [ ] **Step 8: Run stream, executor, and k8s tests**

```bash
rtk docker exec executor-controller go test ./service/deployer ./adapters/k8s ./adapters/redis ./service/handlers -count=1
rtk docker exec k8s-controller go test ./... -count=1
rtk docker exec manifest-controller uv run pytest -v tests/test_streams_contract.py
```

Expected: PASS, including mixed Job/worker capacity integration and promote-seed release.

- [ ] **Step 9: Commit**

```bash
rtk git add pkg/streams pkg/events manifest-controller/streams_contract.py executor-controller k8s-controller
rtk git commit -m "feat: enforce one execution budget across jobs and workers"
```

### Task 9: Implement worker lease lifecycle fan-out

**Files:**
- Create: `pkg/events/task_retry.go`
- Modify: `pkg/events/task_status.go`
- Create: `executor-controller/service/lease/service.go`
- Create: `executor-controller/service/lease/service_test.go`
- Create: `executor-controller/service/lease/fakes_test.go`
- Modify: `executor-controller/service/tasklifecycle/fanout.go`
- Modify: `executor-controller/service/tasklifecycle/fanout_test.go`
- Modify: `executor-controller/domain/event/event.go`
- Modify: `executor-controller/adapters/publisher/outbox_publisher.go`
- Modify: `executor-controller/adapters/publisher/outbox_publisher_test.go`
- Modify: `executor-controller/service/handlers/retry_task_handler.go` and tests
- Modify: `k8s-controller/domain/event/event.go`
- Modify: `k8s-controller/adapters/publisher/outbox_publisher.go` and tests
- Modify: `pkg/streams/contract.yaml` producer descriptions for existing lifecycle streams

- [ ] **Step 1: Write application-service tests**

Tests use an in-memory repository/outbox and prove:

```go
start twice       -> one RUNNING outbox row
success twice     -> one SUCCEEDED + one execution + one node.updated
retryable failure -> FAILED + execution + retry.task, no node.updated
permanent failure -> FAILED + execution + node.updated, no retry.task
stale token       -> ErrStaleLease, zero mutations
upload error      -> warehouse result is not rerun
```

- [ ] **Step 2: Run tests and verify failure**

```bash
rtk docker exec executor-controller go test ./service/lease ./adapters/publisher -count=1
```

Expected: FAIL because the lease service and publisher cases do not exist.

- [ ] **Step 3: Move retry wire shape to pkg**

Define the shared wire exactly, then make k8s-controller and executor-controller both serialize it through one `ToMap`:

```go
type TaskRetry struct {
    TaskID               string `json:"task_id"`
    ScheduleID           string `json:"schedule_id"`
    ScheduleName         string `json:"schedule_name"`
    ServiceName          string `json:"service_name"`
    SchemaName           string `json:"schema_name"`
    TableName            string `json:"table_name"`
    JobName              string `json:"job_name"`
    ImageTag             string `json:"image_tag"`
    RetryCount           int    `json:"retry_count"`
    MaxRetries           int    `json:"max_retries"`
    NodeType             string `json:"node_type"`
    Operation            string `json:"operation,omitempty"`
    ExecutorDeploymentID string `json:"executor_deployment_id,omitempty"`
    DBTUniqueID          string `json:"dbt_unique_id,omitempty"`
    pkgmodel.RuntimeManifestRef
}
```

Update stream contract producers:

```yaml
retry.task:v1 producers: [k8s-controller, executor-controller]
task.execution.recorded:v1 producers: [k8s-controller, executor-controller]
task.status.updated:v1 producers: [executor-controller, k8s-controller, orchestrator]
```

- [ ] **Step 4: Implement atomic claim and heartbeat**

```go
type ClaimInput struct {
    PoolKey string
    Owner   string
    PodName string
    PodUID  string
}

type Grant struct {
    DeploymentID uuid.UUID
    LeaseID      uuid.UUID
    Token        string
    Attempt      int
    ExpiresAt    time.Time
    ExecutionPath model.ExecutionPath
    Argv          []string
    Command       command.DeployTask
}

type StartInput struct {
    DeploymentID uuid.UUID
    LeaseID      uuid.UUID
    Token        string
}

type HeartbeatInput struct {
    DeploymentID uuid.UUID
    LeaseID      uuid.UUID
    Token        string
}

type CompleteInput struct {
    DeploymentID uuid.UUID
    LeaseID      uuid.UUID
    Token        string
    Result       model.WorkerResult
}
```

`Claim` opens a UoW, calls `LockCapacity`, checks `ActiveSlotCount < maxConcurrent`, selects `GetDueWorkerForPool`, resolves/pins argv, generates a 32-byte base64url token and lease UUID, calls aggregate `Claim`, saves, and commits. No due row or no capacity returns `nil,nil`.

`Heartbeat` loads by deployment ID in a transaction, verifies lease ID/token, calls `Heartbeat(now, now+ttl)`, saves, and commits. Identical heartbeats are safe; terminal/cancelled states map to the stable HTTP errors.

- [ ] **Step 5: Implement start transaction**

```go
func (s *Service) Start(ctx context.Context, in StartInput) error {
    return s.withTx(ctx, func(repo repository.DeploymentRepository, outbox outbox.Repository) error {
        dep, err := repo.GetByID(ctx, in.DeploymentID)
        if err != nil { return err }
        changed, err := dep.AcknowledgeStart(in.LeaseID, tokenHash(in.Token), s.clock.Now())
        if err != nil || !changed { return err }
        if err := repo.Save(ctx, dep); err != nil { return err }
        return writeTaskStatus(outbox, dep, "RUNNING", dep.Command().TaskRetryCount)
    })
}
```

The aggregate returns `changed=false` for an identical duplicate start.

- [ ] **Step 6: Implement completion transaction**

Use `lease_id` as `TaskExecutionRecorded.ExecutionID`. Success/permanent failure write exactly:

```go
pkgevents.TaskStatusUpdated
pkgevents.TaskExecutionRecorded
event.NodeUpdated
```

Executor classifies a failed result as retryable only when `result.Retryable` is true, the error class is not `runtime_manifest_rejected`/`runtime_manifest_unverified`/`dbt_unique_id_not_found`/`dbt_selector_not_unique`, and `TaskRetryCount+1 < TaskMaxRetries`. The final allowed attempt is permanent regardless of the worker hint.

Retryable failure writes status/execution plus:

```go
pkgevents.TaskRetry{
    ExecutorDeploymentID: dep.ID().String(),
    TaskID: cmd.TaskID, ScheduleID: cmd.ScheduleID,
    ScheduleName: cmd.ScheduleName, ServiceName: cmd.ServiceName,
    SchemaName: cmd.SchemaName, TableName: cmd.TableName,
    JobName: nextRetryJobName(cmd.JobName, cmd.TaskRetryCount+1),
    ImageTag: cmd.ImageTag, RetryCount: cmd.TaskRetryCount + 1,
    MaxRetries: cmd.TaskMaxRetries, NodeType: cmd.NodeType,
    Operation: cmd.Operation, DBTUniqueID: cmd.DBTUniqueID,
    RuntimeManifestRef: cmd.RuntimeManifestRef,
}
```

The current row becomes `retry_pending` and releases its slot before the outbox row is written. The retry handler requeues this same row after its recorded backoff.

Place the three fan-out variants in `service/tasklifecycle.Fanout` so lease completion and lease-expiry recovery call the same application code:

```go
type Fanout struct{}

func (Fanout) DispatchRejected(ctx context.Context, repo outbox.Repository,
    dep *model.Deployment, reason string) error
func (Fanout) Started(ctx context.Context, repo outbox.Repository,
    dep *model.Deployment) error
func (Fanout) Succeeded(ctx context.Context, repo outbox.Repository,
    dep *model.Deployment, result model.WorkerResult) error
func (Fanout) RetryableFailure(ctx context.Context, repo outbox.Repository,
    dep *model.Deployment, result model.WorkerResult) error
func (Fanout) PermanentFailure(ctx context.Context, repo outbox.Repository,
    dep *model.Deployment, result model.WorkerResult) error
```

Each method builds the exact typed events listed above; it does not open transactions or mutate the aggregate.

`TaskExecutionRecorded.LogS3Key` receives only the object key parsed from `WorkerResult.LogS3URI` (for example `dbt-runs/<schedule>/<task>/<lease>/dbt.log`), matching the current state-service contract. The full log and run-results URIs remain in the deployment's `terminal_result` JSON for executor audit.

- [ ] **Step 7: Add publisher cases**

Executor publisher handles `task_execution_recorded` and `task_retry` through shared `ToMap` methods. Preserve per-aggregate FIFO so RUNNING precedes terminal. K8s publisher switches from its local retry struct to `pkgevents.TaskRetry`.

- [ ] **Step 8: Run tests and commit**

```bash
rtk docker exec executor-controller go test ./service/lease ./service/handlers ./adapters/publisher -count=1
rtk docker exec k8s-controller go test ./domain/event ./adapters/publisher ./service/handlers -count=1
rtk git add pkg/events pkg/streams/contract.yaml executor-controller k8s-controller
rtk git commit -m "feat: complete worker leases through existing lifecycle streams"
```

### Task 10: Expose the authenticated worker API and presigned URLs

**Files:**
- Create: `executor-controller/service/ports/object_url_signer.go`
- Create: `executor-controller/domain/model/worker_pool.go`
- Create: `executor-controller/domain/model/worker_pool_test.go`
- Create: `executor-controller/domain/repository/worker_pool.go`
- Create: `executor-controller/adapters/postgres/worker_pool_repository.go`
- Create: `executor-controller/adapters/postgres/worker_pool_repository_test.go`
- Create: `executor-controller/adapters/s3/presigner.go`
- Create: `executor-controller/adapters/s3/presigner_test.go`
- Create: `executor-controller/adapters/http/dto.go`
- Create: `executor-controller/adapters/http/auth.go`
- Create: `executor-controller/adapters/http/auth_test.go`
- Create: `executor-controller/adapters/http/worker_handler.go`
- Create: `executor-controller/adapters/http/worker_handler_test.go`
- Create: `executor-controller/adapters/http/server.go`
- Modify: `executor-controller/adapters/http/health.go`
- Create: `executor-controller/service/workerapi/authenticator.go`
- Create: `executor-controller/service/workerapi/authenticator_test.go`
- Modify: `executor-controller/config/config.go`
- Modify: `executor-controller/config/config_test.go`
- Modify: `executor-controller/main.go`
- Modify: `executor-controller/go.mod`
- Modify: `executor-controller/go.sum`

- [ ] **Step 1: Write HTTP contract tests**

Use `httptest` to assert:

```text
GET  /internal/v1/worker/runtime              -> descriptor_url + artifact_url
POST /internal/v1/workers/claim               -> 200 lease or 204 timeout
POST /internal/v1/leases/{id}/start           -> 200, duplicate 200
POST /internal/v1/leases/{id}/heartbeat       -> 200, stale 409, cancelled 410
POST /internal/v1/leases/{id}/result-urls     -> two PUT URLs/S3 URIs
POST /internal/v1/leases/{id}/complete        -> 200, stale 409
POST /internal/v1/workers/initialization      -> records/clears pool error and hydration duration
bad/missing pool credential                   -> 401
credential for another pool                   -> 403
malformed JSON                                -> 400 with stable code
```

Example error body:

```json
{"error":{"code":"stale_lease","message":"lease token is no longer current"}}
```

- [ ] **Step 2: Run HTTP tests**

```bash
rtk docker exec executor-controller go test ./adapters/http ./adapters/s3 ./adapters/postgres -run 'Worker|Presign|PoolCredential' -count=1
```

Expected: FAIL because the packages do not exist.

- [ ] **Step 3: Implement pool-scoped authentication**

Define the domain record and repository:

```go
type WorkerPool struct {
    PoolKey             string
    ServiceName         string
    ImageTag            string
    RuntimeManifest     pkgmodel.RuntimeManifestRef
    CredentialSHA256    string
    DesiredReplicas     int
    LastActivityAt      time.Time
    InitializationError string
    CreatedAt           time.Time
    UpdatedAt           time.Time
}

type WorkerPoolRepository interface {
    Get(ctx context.Context, poolKey string) (*model.WorkerPool, error)
    Add(ctx context.Context, pool model.WorkerPool) error
    Save(ctx context.Context, pool model.WorkerPool) error
    List(ctx context.Context) ([]model.WorkerPool, error)
}
```

`service/workerapi.Authenticator` loads pool records through the domain repository port and compares:

```go
func VerifyCredential(raw string, expectedSHA string) bool {
    sum := sha256.Sum256([]byte(raw))
    actual := hex.EncodeToString(sum[:])
    return subtle.ConstantTimeCompare([]byte(actual), []byte(expectedSHA)) == 1
}
```

Requests send `X-Continuo-Pool-Key` and `Authorization: Bearer <credential>`. Middleware places the verified pool record in request context. Never log the header or presigned URLs.

The HTTP adapter depends on the authenticator interface and lease use-case service; it never imports `adapters/postgres` or constructs repositories.

- [ ] **Step 4: Implement S3-compatible presigning behind a port**

```go
type ObjectURLSigner interface {
    PresignGet(ctx context.Context, s3URI string, ttl time.Duration) (string, error)
    PresignPut(ctx context.Context, s3URI, contentType string, ttl time.Duration) (string, error)
}
```

Use AWS SDK v2 `s3.NewPresignClient`, configured endpoint, path-style addressing for LocalStack, optional static credentials, and a 15-minute URL TTL. Parse only `s3://bucket/nonempty-key`.

- [ ] **Step 5: Implement stable artifact/result locations**

Runtime response derives descriptor URI by replacing the msgpack basename with `runtime-manifest.json` and presigns both GETs. Result keys are:

```text
s3://<bucket>/dbt-runs/<schedule_id>/<task_id>/<lease_id>/dbt.log
s3://<bucket>/dbt-runs/<schedule_id>/<task_id>/<lease_id>/run_results.json
```

Return both URL and canonical S3 URI. Completion accepts only the exact URIs issued for that lease.

- [ ] **Step 6: Implement claim long-poll**

The handler calls `lease.Service.Claim` immediately, then every 500ms until `min(request.wait_seconds, configuredClaimWait)` or request cancellation. A pool initialization error returns `409 pool_not_ready`. Empty demand returns `204`.

Lease response:

```json
{
  "deployment_id":"uuid",
  "lease_id":"uuid",
  "lease_token":"raw-once",
  "attempt":1,
  "expires_at":"RFC3339Nano",
  "execution_path":"native",
  "argv":["dbt","run","--select","orders"],
  "task":{
    "task_id":"uuid",
    "schedule_id":"uuid",
    "service_name":"finance",
    "schema_name":"public",
    "table_name":"orders",
    "dbt_unique_id":"model.finance.orders"
  }
}
```

- [ ] **Step 7: Compose one HTTP server**

`NewServer` installs health/ready plus internal handlers on one `http.ServeMux`. Keep the same port and shutdown behavior; remove no existing endpoint.

Add `S3 pkgconfig.S3Config` to executor config and construct the presigner, authenticator, lease service, and combined HTTP server in `main.go` in this task. Pool reconciliation is still absent, so no team pod starts, but the internal API is live and testable at this checkpoint.

- [ ] **Step 8: Run tests and update Slice 2 docs**

```bash
rtk docker compose build executor-controller
rtk docker exec executor-controller go test ./... -count=1
```

Modify:

```text
docs/arch/02-interaction-matrix.md
docs/arch/03-sequence-flows.md
docs/arch/04-service-ownership.md
docs/arch/streams.md
docs/arch/services/executor-controller.md
docs/arch/services/k8s-controller.md
```

Document pull-only HTTP, credential scope, lease fencing/idempotency, same-row retries, capacity-only stream, two-phase Job reservation, and lifecycle ownership.

- [ ] **Step 9: Commit**

```bash
rtk git add executor-controller docs/arch/02-interaction-matrix.md docs/arch/03-sequence-flows.md docs/arch/04-service-ownership.md docs/arch/streams.md docs/arch/services/executor-controller.md docs/arch/services/k8s-controller.md
rtk git commit -m "feat: expose fenced worker lease API"
```

---

## Slice 3 — Base-image worker runtime

### Task 11: Hydrate and validate one Manifest per worker process

**Files:**
- Create: `dbt/base/continuo_dbt_runtime/api_client.py`
- Create: `dbt/base/continuo_dbt_runtime/artifact_store.py`
- Create: `dbt/base/continuo_dbt_runtime/execution.py`
- Create: `dbt/base/continuo_dbt_runtime/worker.py`
- Create: `dbt/base/continuo_dbt_runtime/__main__.py`
- Create: `dbt/base/bin/continuo-dbt-worker`
- Modify: `dbt/base/Dockerfile`
- Create: `dbt/tests/test_worker_api_client.py`
- Create: `dbt/tests/test_worker_execution.py`
- Create: `dbt/tests/test_worker_loop.py`

- [ ] **Step 1: Write artifact and API client tests**

Tests use a local `ThreadingHTTPServer` and cover retries, 204 claim, 409 stale lease, redacted errors, checksum mismatch, version mismatch, context mismatch, adapter mismatch, image/service mismatch, malformed msgpack, and exact dbt-ID lookup.

```python
def test_artifact_store_hydrates_once_and_rejects_parser_fallback(tmp_path, monkeypatch):
    server = fake_executor_with_runtime_artifact(real_manifest_bytes())
    store = ArtifactStore(config_for(server, tmp_path))
    loaded = store.load()
    assert loaded.manifest.nodes["model.service_1.table_a"].name == "table_a"
    assert server.count("/artifact") == 1
    assert store.load() is loaded
```

- [ ] **Step 2: Run tests and verify failure**

```bash
rtk docker build -t dbt-base:latest dbt/base
rtk docker compose build dbt-compile-and-load
rtk docker compose up -d dbt-compile-and-load
rtk docker exec dbt-compile-and-load python -m pytest -v tests/test_worker_api_client.py tests/test_worker_execution.py tests/test_worker_loop.py
```

Expected: FAIL because the worker modules do not exist.

- [ ] **Step 3: Implement a stdlib-only HTTP client**

```python
class ExecutorClient:
    def __init__(self, base_url: str, pool_key: str, credential: str,
                 timeout_seconds: float = 30.0):
        self._base_url = base_url.rstrip("/")
        self._pool_key = pool_key
        self._credential = credential
        self._timeout = timeout_seconds

    def request(self, method: str, path: str, body: dict | None = None,
                lease_token: str | None = None) -> tuple[int, dict | None]:
        headers = {
            "Authorization": f"Bearer {self._credential}",
            "X-Continuo-Pool-Key": self._pool_key,
            "Content-Type": "application/json",
        }
        if lease_token is not None:
            headers["X-Continuo-Lease-Token"] = lease_token
        data = None if body is None else json.dumps(body).encode()
        request = urllib.request.Request(
            f"{self._base_url}{path}", data=data, headers=headers, method=method
        )
        try:
            with urllib.request.urlopen(request, timeout=self._timeout) as response:
                raw = response.read()
                return response.status, json.loads(raw) if raw else None
        except urllib.error.HTTPError as exc:
            raw = exc.read()
            parsed = json.loads(raw) if raw else None
            return exc.code, parsed
```

Add typed methods `runtime()`, `claim()`, `start()`, `heartbeat()`, `result_urls()`, `complete()`, and `initialization()`. Retry only connection errors/`429`/`5xx` with bounded exponential backoff; never retry `400/401/403/409` as a new request.

- [ ] **Step 4: Implement artifact startup validation**

```python
@dataclass(frozen=True)
class LoadedArtifact:
    manifest: Manifest
    canonical_path: Path
    descriptor: dict


class ArtifactStore:
    def load(self) -> LoadedArtifact:
        if self._loaded is not None:
            return self._loaded
        runtime = self._client.runtime()
        descriptor = download_json(runtime["descriptor_url"])
        validate_descriptor(
            descriptor,
            expected_service=self._config.service_name,
            expected_image_tag=self._config.image_tag,
            expected_ref=self._config.runtime_ref,
        )
        packed = download_bytes(runtime["artifact_url"])
        if hashlib.sha256(packed).hexdigest() != descriptor["sha256"]:
            raise InitializationError("runtime_manifest_checksum_mismatch")
        installed = importlib.metadata.version("dbt-core")
        if installed != descriptor["dbt_core_version"]:
            raise InitializationError("runtime_manifest_dbt_version_mismatch")
        manifest = Manifest.from_msgpack(packed)
        service_nodes = [
            node for node in manifest.nodes.values()
            if node.resource_type in {"model", "seed", "snapshot"}
            and node.fqn
            and node.fqn[0].replace("_", "-") == self._config.service_name
        ]
        if not service_nodes:
            raise InitializationError("runtime_manifest_service_nodes_missing")
        actual_context = parse_context_sha256(
            manifest, self._config.controller_context_json
        )
        if actual_context != descriptor["parse_context_sha256"]:
            raise InitializationError("runtime_manifest_parse_context_mismatch")
        self._config.cache_dir.mkdir(parents=True, exist_ok=True)
        canonical = self._config.cache_dir / "partial_parse.msgpack"
        canonical.write_bytes(packed)
        self._loaded = LoadedArtifact(manifest, canonical, descriptor)
        return self._loaded
```

Validate `adapter_type == "postgres"` and at least one model/seed/snapshot node. Do not inspect project SQL files or invoke dbt parsing.

- [ ] **Step 5: Implement task environment isolation**

```python
@contextmanager
def task_environment(values: dict[str, str]):
    before = {key: os.environ.get(key) for key in values}
    try:
        os.environ.update(values)
        yield
    finally:
        for key, value in before.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
```

Task values include existing Job contract fields (`TASK_ID`, `SCHEDULE_ID`, `SCHEDULE_NAME`, `SERVICE_NAME`, `SCHEMA`, `TABLE_NAME`, `JOB_NAME`) and task-local `DBT_TARGET_PATH`/`DBT_LOG_PATH`. Pool credential is never part of this map.

- [ ] **Step 6: Implement native execution with a supplied Manifest**

```python
class NativeExecutor:
    def __init__(self, artifact: LoadedArtifact, event_sink: EventSink):
        self._manifest = artifact.manifest
        self._runner = dbtRunner(manifest=self._manifest, callbacks=[event_sink])

    def execute(self, lease: Lease, task_dir: Path) -> ExecutionResult:
        if lease.task.dbt_unique_id not in self._manifest.nodes:
            return ExecutionResult.permanent(
                "dbt_unique_id_not_found",
                f"{lease.task.dbt_unique_id} is absent from the pinned Manifest",
            )
        selected = [
            unique_id for unique_id, node in self._manifest.nodes.items()
            if node.name == lease.task.table_name
            and node.resource_type in {"model", "seed", "snapshot"}
        ]
        if selected != [lease.task.dbt_unique_id]:
            return ExecutionResult.permanent(
                "dbt_selector_not_unique",
                f"selector name resolves to {selected!r}",
            )
        if Path(lease.argv[0]).name != "dbt":
            raise ValueError("native executor received non-dbt argv")
        result = self._runner.invoke(lease.argv[1:])
        if isinstance(result.result, RunExecutionResult):
            result.result.write(str(task_dir / "run_results.json"))
        if result.exception is not None:
            return ExecutionResult.unsafe("dbt_runner_exception", str(result.exception))
        return ExecutionResult(succeeded=result.success)
```

After each invocation call `cleanup_connections()` and `reset_adapters()`. If cleanup/reset or any unexpected exception fails, complete/fence the lease and exit so Kubernetes starts a clean process. Connection reuse is therefore not a correctness or performance assumption.

- [ ] **Step 7: Prove project parsing is impossible on the native path**

Patch the actual dbt parser seam:

```python
def forbidden_parse(*args, **kwargs):
    raise AssertionError("ManifestLoader.get_full_manifest must not run")

monkeypatch.setattr(
    "dbt.parser.manifest.ManifestLoader.get_full_manifest",
    forbidden_parse,
)
result = NativeExecutor(loaded_artifact, sink).execute(native_lease(), tmp_path)
assert result.succeeded
```

The fixture must execute a real `dbt run --select table_a` against the test Postgres, not only mock `dbtRunner`.

- [ ] **Step 8: Build the worker loop**

Startup sequence:

```python
credential = os.environ.pop("CONTINUO_POOL_CREDENTIAL")
client = ExecutorClient(config.executor_url, config.pool_key, credential)
try:
    artifact = ArtifactStore(config, client).load()
except InitializationError as exc:
    client.initialization(ok=False, error_code=exc.code, message=str(exc))
    wait_unready_until_signal()
    raise SystemExit(0)
client.initialization(ok=True)
config.ready_file.write_text("ready\n")
```

An invalid artifact therefore remains unready without a CrashLoop. Then claim one lease, start, execute, upload results, complete, and repeat sequentially. Install SIGTERM/SIGINT handlers that stop new claims and cancel the active execution before exiting.

- [ ] **Step 9: Install the worker command**

```dockerfile
COPY bin/continuo-dbt-worker /continuo/bin/continuo-dbt-worker
RUN chmod 0555 /continuo/bin/continuo-dbt-worker
```

Keep the image's existing entrypoint unchanged.

- [ ] **Step 10: Run tests and commit**

```bash
rtk docker build -t dbt-base:latest dbt/base
rtk docker compose build dbt-compile-and-load
rtk docker compose up -d dbt-compile-and-load
rtk docker exec dbt-compile-and-load python -m pytest -v tests/test_runtime_artifact.py tests/test_worker_api_client.py tests/test_worker_execution.py tests/test_worker_loop.py
rtk git add dbt/base dbt/Dockerfile.upload dbt/tests
rtk git commit -m "feat: run dbt nodes from one hydrated manifest"
```

### Task 12: Preserve exact custom-wrapper behavior and isolate result uploads

**Files:**
- Modify: `dbt/base/continuo_dbt_runtime/execution.py`
- Modify: `dbt/base/continuo_dbt_runtime/worker.py`
- Create: `dbt/base/continuo_dbt_runtime/result_upload.py`
- Modify: `dbt/tests/test_worker_execution.py`
- Modify: `dbt/tests/test_worker_loop.py`
- Create: `dbt/tests/fixtures/fake_dbt_wrapper.py`

- [ ] **Step 1: Write wrapper contract tests**

Assert exact argv/cwd, environment restoration, task-local cache copy, credential absence, cache acceptance, every rejection code, missing acceptance, opaque behavior, stdout/stderr capture, process-tree cancellation, and successful mutation despite failed log upload.

```python
def test_wrapper_receives_exact_argv_and_no_pool_secret(tmp_path):
    lease = wrapper_lease(["wise-dbt", "run-model", "orders"])
    result = WrapperExecutor(artifact, required_cache=True).execute(lease, tmp_path)
    invocation = json.loads((tmp_path / "invocation.json").read_text())
    assert invocation["argv"] == ["wise-dbt", "run-model", "orders"]
    assert invocation["cwd"] == "/project"
    assert "CONTINUO_POOL_CREDENTIAL" not in invocation["env"]
    assert invocation["env"]["DBT_PARTIAL_PARSE_FILE_PATH"].startswith(str(tmp_path))
```

- [ ] **Step 2: Run wrapper tests**

```bash
rtk docker exec dbt-compile-and-load python -m pytest -v tests/test_worker_execution.py -k wrapper
```

Expected: FAIL because only native execution exists.

- [ ] **Step 3: Execute wrappers without a shell**

Run the same exact-ID/name uniqueness guard as the native executor before starting the child.

```python
task_cache = task_dir / "partial_parse.msgpack"
shutil.copyfile(self._artifact.canonical_path, task_cache)
env = os.environ.copy()
env.update({
    "DBT_PARTIAL_PARSE_FILE_PATH": str(task_cache),
    "DBT_LOG_FORMAT": "json",
    "DBT_LOG_LEVEL": "debug",
    "DBT_TARGET_PATH": str(task_dir / "target"),
    "DBT_LOG_PATH": str(task_dir / "logs"),
})
for key in POOL_SECRET_ENV_KEYS:
    env.pop(key, None)
process = subprocess.Popen(
    lease.argv,
    cwd="/project",
    env=env,
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE,
    text=True,
    start_new_session=True,
)
```

Stream both pipes into the task log without changing bytes presented to the wrapper. Never use `shell=True` and never translate argv.

- [ ] **Step 4: Enforce structured cache evidence**

Parse JSON event `info.code` values:

```python
CACHE_ACCEPTED = {"I017"}  # no changes; parsing skipped
CACHE_REJECTED = {"I016", "I024", "I028", "I040"}
```

For `wrapper_required`: terminate the process group immediately on a rejected code; after exit require exactly one accepted observation. Missing acceptance returns `runtime_manifest_unverified`. For `wrapper_opaque`, record observations but do not require dbt events. Neither path is labeled native.

- [ ] **Step 5: Implement cancellation**

Heartbeat `409 stale_lease` or `410 cancelled` sets a cancellation event. Wrapper cancellation sends `SIGTERM` to `os.getpgid(pid)`, waits five seconds, then sends `SIGKILL`. Native cancellation calls dbt's active adapter cancellation where available, marks runtime unsafe, and exits the pod after reporting.

- [ ] **Step 6: Upload after dbt, without rerunning dbt**

`ResultUploader` obtains URLs once, uploads log and optional run results with HTTP PUT, and retries transport/`5xx` failures while the heartbeat continues. After a bounded 60-second upload window:

```python
if execution.succeeded and upload.failed:
    logger.error("result upload failed after successful dbt execution", extra=upload.safe_fields())
    completion = execution.with_artifact_error(upload.error_code)
else:
    completion = execution.with_uris(upload.log_uri, upload.run_results_uri)
client.complete(lease, completion)
```

An upload failure never calls `execute` again and never changes a successful warehouse result to retryable.

- [ ] **Step 7: Run all worker tests**

```bash
rtk docker exec dbt-compile-and-load python -m pytest -v tests/test_runtime_artifact.py tests/test_worker_api_client.py tests/test_worker_execution.py tests/test_worker_loop.py
```

Expected: PASS.

- [ ] **Step 8: Commit**

```bash
rtk git add dbt/base/continuo_dbt_runtime dbt/tests
rtk git commit -m "feat: execute qualified dbt wrappers from promoted cache"
```

### Task 13: Qualify real dbt semantics in the worker image

**Files:**
- Create: `dbt/services/service-1/models/worker_view.sql`
- Create: `dbt/services/service-1/models/worker_incremental.sql`
- Create: `dbt/services/service-1/snapshots/worker_snapshot.sql`
- Modify: `dbt/services/service-1/dbt_project.yml`
- Create: `dbt/tests/test_worker_dbt_semantics.py`
- Modify: `dbt/services/service-1/Dockerfile`
- Modify: `dbt/services/service-1/Dockerfile.local`

- [ ] **Step 1: Add minimal semantic fixtures**

`worker_incremental.sql`:

```sql
{{ config(materialized='incremental', unique_key='id') }}

select 1 as id, 'current'::text as value
{% if is_incremental() %}
where not exists (select 1 from {{ this }} where id = 1)
{% endif %}
```

`worker_view.sql` sets `materialized='view'` and selects from the incremental model. `worker_snapshot.sql` uses check strategy, `unique_key='id'`, and selects from `ref('worker_incremental')`. Give fixtures one dedicated `worker-runtime` tag and required owner metadata.

- [ ] **Step 2: Write container integration tests**

Compile once, hydrate once, then run:

```text
table first run
view
incremental first run
incremental existing run
seed
snapshot
test
build
```

Assert relation types, incremental row count stays one, snapshot history exists, and `ManifestLoader.get_full_manifest` remains fatal for every native invocation.

- [ ] **Step 3: Run against Docker Postgres**

```bash
rtk docker compose up -d postgres
rtk docker build -t dbt-base:latest dbt/base
rtk docker build -t service-1:worker-test -f dbt/services/service-1/Dockerfile.local dbt/services/service-1
rtk docker compose build dbt-compile-and-load
rtk docker compose up -d dbt-compile-and-load
rtk docker exec dbt-compile-and-load python -m pytest -v tests/test_worker_dbt_semantics.py
```

Expected: PASS for all eight semantics and one hydration count.

- [ ] **Step 4: Verify dormant batch mode**

```bash
rtk docker image inspect dbt-base:latest --format '{{json .Config.Entrypoint}}'
rtk docker run --rm --entrypoint sh dbt-base:latest -c 'test -x /entrypoint.sh && test -x /continuo/bin/continuo-dbt-worker && test -x /continuo/bin/continuo-export-runtime-manifest'
```

Expected: entrypoint is `["/entrypoint.sh"]` and no worker process starts automatically.

- [ ] **Step 5: Update Slice 3 docs and commit**

Modify `docs/arch/services/executor-controller.md` sequence appendix with native/wrapper execution, cache event codes, environment isolation, and upload-after-execution behavior.

```bash
rtk git add dbt/services/service-1 dbt/tests/test_worker_dbt_semantics.py docs/arch/services/executor-controller.md
rtk git commit -m "test: qualify dbt semantics in reusable workers"
```

---

## Slice 4 — Kubernetes pools, failure handling, rollout, and acceptance

### Task 14: Reconcile reusable worker Deployments and Secrets

**Files:**
- Create: `executor-controller/domain/workerpool/pool.go`
- Create: `executor-controller/domain/workerpool/pool_test.go`
- Create: `executor-controller/service/ports/worker_pool_runtime.go`
- Create: `executor-controller/service/pool/reconciler.go`
- Create: `executor-controller/service/pool/reconciler_test.go`
- Create: `executor-controller/adapters/k8s/worker_pools.go`
- Create: `executor-controller/adapters/k8s/worker_pools_test.go`
- Modify: `executor-controller/adapters/k8s/client.go`
- Modify: `executor-controller/adapters/postgres/worker_pool_repository.go`
- Modify: `executor-controller/domain/repository/worker_pool.go`

- [ ] **Step 1: Write allocation and Kubernetes-shape tests**

Cover oldest-ready cross-pool allocation, configurable limits, zero demand, busy no-downscale, idle zero-scale, old-pool restart, credential rotation after Secret loss, separate image/SHA pools, initialization-error replica cap, pending-worker rollback to Jobs, and no Service resource.

```go
func TestDesiredReplicasNeverReducesBusyPool(t *testing.T) {
    got := workerpool.DesiredReplicas(workerpool.ScaleInput{
        CurrentReplicas: 5, ActiveLeases: 2, AllocatedPending: 0,
        LastActivityAt: time.Unix(0, 0), Now: time.Unix(1000, 0),
        IdleTimeout: time.Minute,
    })
    assert.Equal(t, 5, got)
}
```

- [ ] **Step 2: Run tests and verify failure**

```bash
rtk docker exec executor-controller go test ./domain/workerpool ./service/pool ./adapters/k8s -run 'Pool|WorkerDeployment|DesiredReplica' -count=1
```

Expected: FAIL because pool reconciliation does not exist.

- [ ] **Step 3: Implement fair allocation**

```go
func Allocate(demand []model.PoolDemand, activeSlots, limit int) map[string]int {
    available := max(0, limit-activeSlots)
    sort.SliceStable(demand, func(i, j int) bool {
        return demand[i].OldestReadyAt.Before(demand[j].OldestReadyAt)
    })
    allocated := make(map[string]int)
    for _, pool := range demand {
        n := min(pool.Pending, available)
        allocated[pool.PoolKey] = n
        available -= n
        if available == 0 { break }
    }
    return allocated
}
```

Desired replicas:

```go
if input.ActiveLeases > 0 {
    return max(input.CurrentReplicas, input.ActiveLeases+input.AllocatedPending)
}
if input.AllocatedPending > 0 {
    return input.AllocatedPending
}
if input.Now.Sub(input.LastActivityAt) >= input.IdleTimeout {
    return 0
}
return input.CurrentReplicas
```

If `initialization_error` is nonempty, cap desired replicas at one unready diagnostic pod. On reconcile, if the current mode policy for a pool's service is now `jobs`, atomically convert only that pool's `pending`/`retry_pending` records to `execution_mode='jobs'` and clear `pool_key`. Preserve any already-resolved argv so the Job retry executes the same command. Never convert `leased`/`running` work; it must finish or be cancelled/fenced first.

- [ ] **Step 4: Define the Kubernetes port**

```go
type WorkerPoolSpec struct {
    PoolKey              string
    ServiceName          string
    ImageTag             string
    RuntimeManifest      pkgmodel.RuntimeManifestRef
    ControllerContextJSON string
    Credential           string
    DesiredReplicas      int32
}

type WorkerPoolRuntime interface {
    Ensure(ctx context.Context, spec WorkerPoolSpec) error
    Status(ctx context.Context, poolKey string) (PoolStatus, bool, error)
    DeletePod(ctx context.Context, podName, podUID string) error
}

type PoolStatus struct {
    DesiredReplicas int
    ReadyReplicas   int
}
```

- [ ] **Step 5: Build one Deployment and Secret per pool**

Name resources `dbt-worker-<first-16-pool-key>`. The Secret contains one key `credential`. The Deployment exposes it only to the worker bootstrap through `secretKeyRef`; bootstrap immediately pops it from `os.environ` before loading dbt:

```yaml
spec:
  replicas: <desired>
  strategy:
    type: RollingUpdate
    rollingUpdate: {maxUnavailable: 0, maxSurge: 1}
  template:
    spec:
      containers:
        - name: worker
          image: <same team image currently used by Jobs>
          command: ["/continuo/bin/continuo-dbt-worker"]
          readinessProbe:
            exec:
              command: ["sh", "-c", "test -f /tmp/continuo-worker-ready"]
          env:
            - {name: CONTINUO_EXECUTOR_URL, value: <configured internal URL>}
            - {name: CONTINUO_POOL_KEY, value: <full pool key>}
            - {name: CONTINUO_SERVICE_NAME, value: <service>}
            - {name: CONTINUO_IMAGE_TAG, value: <image tag>}
            - {name: CONTINUO_RUNTIME_CONTEXT_JSON, value: <canonical JSON>}
            - name: CONTINUO_POOL_CREDENTIAL
              valueFrom:
                secretKeyRef: {name: <pool secret>, key: credential}
            - name: CONTINUO_POD_NAME
              valueFrom: {fieldRef: {fieldPath: metadata.name}}
            - name: CONTINUO_POD_UID
              valueFrom: {fieldRef: {fieldPath: metadata.uid}}
```

Also pass the same warehouse environment as current production Jobs. Add labels `app=continuo-dbt-worker` and truncated pool key; store the full key/ref in annotations. Do not create a Service.

- [ ] **Step 6: Reconcile credentials safely**

On new pool, generate 32 random bytes, base64url encode, persist SHA in `executor_worker_pools`, and create Secret with raw credential. If DB row exists but Secret does not, rotate both in one reconcile attempt. Never return the raw value from a repository or log it.

- [ ] **Step 7: Run tests and commit**

```bash
rtk docker exec executor-controller go test ./domain/workerpool ./service/pool ./adapters/k8s ./adapters/postgres -count=1
rtk git add executor-controller/domain/workerpool executor-controller/service/pool executor-controller/service/ports/worker_pool_runtime.go executor-controller/adapters/k8s executor-controller/adapters/postgres/worker_pool_repository.go executor-controller/domain/repository/worker_pool.go
rtk git commit -m "feat: reconcile reusable dbt worker pools"
```

### Task 15: Fence crashes and cancel active worker pods

**Files:**
- Create: `executor-controller/service/reaper/reaper.go`
- Create: `executor-controller/service/reaper/reaper_test.go`
- Create: `executor-controller/service/ports/pod_terminator.go`
- Modify: `executor-controller/service/handlers/schedule_cancelled_handler.go`
- Modify: `executor-controller/service/handlers/schedule_cancelled_handler_test.go`
- Modify: `executor-controller/adapters/redis/schedule_cancelled_binding.go`
- Modify: `executor-controller/domain/repository/cancelled_schedules.go`
- Modify: `executor-controller/adapters/postgres/cancelled_schedules_repository.go`
- Modify: `executor-controller/service/uow/uow.go`
- Modify: `executor-controller/service/uow/fake.go`
- Modify: `executor-controller/adapters/k8s/worker_pools.go`

- [ ] **Step 1: Write failure and cancellation tests**

```text
expired pending lease -> fence token -> request exact pod UID deletion -> release slot -> retry_pending
expired final attempt -> fence/delete -> permanent FAILED fan-out
delete API failure -> transaction rolls back; no new worker can claim
pending schedule cancellation -> cancelled without pod deletion
active cancellation -> fence/delete -> cancelled and slot released
late completion after either path -> stale_lease
duplicate cancellation -> no-op
```

- [ ] **Step 2: Run tests and verify failure**

```bash
rtk docker exec executor-controller go test ./service/reaper ./service/handlers -run 'ExpiredLease|ScheduleCancelled|Fence' -count=1
```

Expected: FAIL because active lease termination is absent.

- [ ] **Step 3: Reuse the inward-owned cancelled-schedule port**

```go
type CancelledSchedulesRepository interface {
    Insert(ctx context.Context, scheduleID uuid.UUID) error
    Exists(ctx context.Context, scheduleID uuid.UUID) (bool, error)
    DeleteExpired(ctx context.Context, ttl time.Duration) (int64, error)
}
```

Keep it under `domain/repository` as established in Task 6. The Postgres adapter's compile-time assertion remains, and cancellation changes add no application/UoW import of `adapters/postgres`.

- [ ] **Step 4: Implement fenced expiry**

For one expired row per transaction:

```go
dep, err := repo.GetExpiredLeaseForUpdate(ctx, now)
if err != nil || dep == nil { return err }
lease := dep.ActiveLease()
if err := dep.Fence(lease.ID, now, "lease_expired"); err != nil { return err }
if err := terminator.DeletePod(ctx, lease.PodName, lease.PodUID); err != nil {
    return err // transaction rollback leaves current lease authoritative
}
if dep.Command().TaskRetryCount+1 < dep.Command().TaskMaxRetries {
    dep.MarkRetryPending(now, backoff)
    result := model.WorkerResult{
        Succeeded: false, Retryable: true,
        ErrorClass: "worker_lease_expired",
        ErrorMessage: "worker heartbeat expired",
    }
    if err := r.fanout.RetryableFailure(ctx, outboxRepo, dep, result); err != nil {
        return err
    }
} else {
    dep.MarkFailed(now, "worker lease expired")
    result := model.WorkerResult{
        Succeeded: false, Retryable: false,
        ErrorClass: "worker_lease_expired",
        ErrorMessage: "worker heartbeat expired",
    }
    if err := r.fanout.PermanentFailure(ctx, outboxRepo, dep, result); err != nil {
        return err
    }
}
return repo.Save(ctx, dep)
```

Kubernetes deletion uses a UID precondition so a recycled pod name cannot terminate a replacement.

- [ ] **Step 5: Make cancellation transactional**

`ScheduleCancelledHandler` begins UoW, inserts the cancellation tombstone, calls `CancelSchedule`, and for each active lease fences then deletes the exact pod before saving cancelled/releasing slot. Pending rows are marked cancelled directly. Commit only after all requested deletions are accepted; duplicate handler calls see terminal rows.

- [ ] **Step 6: Run tests and commit**

```bash
rtk docker exec executor-controller go test ./service/reaper ./service/handlers ./service/uow ./adapters/postgres ./adapters/k8s -count=1
rtk git add executor-controller
rtk git commit -m "feat: fence expired and cancelled worker leases"
```

### Task 16: Wire worker mode, configuration, RBAC, and telemetry

**Files:**
- Modify: `executor-controller/main.go`
- Create: `executor-controller/service/telemetry/metrics.go`
- Create: `executor-controller/service/telemetry/metrics_test.go`
- Modify: `executor-controller/config/config.go` and tests
- Modify: `executor-controller/go.mod` and `go.sum`
- Modify: `docker-compose.yml`
- Modify: `deploy/app/values.yaml`
- Modify: `deploy/app/templates/deployment.yaml`
- Modify: `tests/e2e/k8s/executor-controller-deployment.yaml`
- Modify: `scripts/setup.sh`
- Modify: `tests/e2e/provision-k8s-test-env.sh`
- Modify: `tests/e2e/cleanup-k8s-controllers.sh`

- [ ] **Step 1: Write wiring/config/RBAC guard tests**

Add tests that parse Compose/Helm/e2e manifests and assert required env values, positive `50` deployment value, Apps/Deployments + Core/Secrets/Pods RBAC, and no worker Service. Add a main wiring test for the Job terminal consumer and all worker goroutines.

- [ ] **Step 2: Run guards and verify failure**

```bash
rtk docker exec executor-controller go test ./config ./service/telemetry ./adapters/commandcfg ./adapters/k8s -run 'Config|DeploymentConfig|RBAC|Telemetry' -count=1
```

Expected: FAIL because deployment configuration is still Job-only.

- [ ] **Step 3: Wire dependencies in main**

Initialize in this order:

```text
Postgres + UoW/repositories
command resolver + runtime-context builder
S3 presigner
Kubernetes client implementing Job deployer and worker-pool runtime
lease service
worker HTTP server
executor.job.terminal consumer
outbox processor
Jobs dispatcher
pool reconciler
lease/dispatching reaper
cancelled-schedule sweeper
existing Redis consumers
```

All goroutines use root context and lifecycle shutdown. The HTTP ready endpoint is unhealthy if DB is unavailable; worker initialization failures do not make the controller itself unready.

- [ ] **Step 4: Add OpenTelemetry instruments**

```go
type Metrics struct {
    ClaimLatency             metric.Float64Histogram
    LeaseWait                metric.Float64Histogram
    ArtifactHydration        metric.Float64Histogram
    ReadyToDBTStart          metric.Float64Histogram
    ExecutionDuration        metric.Float64Histogram
    UploadDuration           metric.Float64Histogram
    HeartbeatExpiries        metric.Int64Counter
    ScaleUps                 metric.Int64Counter
    ScaleToZero              metric.Int64Counter
    CacheAccepted            metric.Int64Counter
    CacheRejected            metric.Int64Counter
}
```

Register observable gauges for pending/oldest-ready by pool, desired/ready replicas, and active slots/limit. Attributes contain service, execution path, and short pool key; never task IDs, credentials, URLs, or full artifact hashes.

- [ ] **Step 5: Replace deployment configuration**

Compose, Helm, and e2e set:

```yaml
EXECUTION_MODE: "jobs"
EXECUTION_MODE_OVERRIDES_JSON: "{}"
MAX_CONCURRENT_EXECUTIONS: "50"
WORKER_IDLE_TIMEOUT_SECONDS: "300"
WORKER_LEASE_TTL_SECONDS: "60"
WORKER_HEARTBEAT_INTERVAL_SECONDS: "15"
WORKER_CLAIM_WAIT_SECONDS: "20"
WORKER_CONTROL_PLANE_URL: "http://executor-controller:8084"
```

Remove deployed `MAX_CONCURRENT_JOBS`. Keep alias code for one release. For e2e canary use `{"service-1":"workers"}` only in worker-specific test setup, not the global default manifest.

- [ ] **Step 6: Extend RBAC**

Executor ServiceAccount needs:

```yaml
- apiGroups: ["apps"]
  resources: ["deployments"]
  verbs: ["get", "list", "watch", "create", "update", "patch"]
- apiGroups: [""]
  resources: ["secrets"]
  verbs: ["get", "create", "update", "patch"]
- apiGroups: [""]
  resources: ["pods"]
  verbs: ["get", "list", "delete"]
```

Retain existing Job permissions. Worker pods use no Kubernetes RBAC.

- [ ] **Step 7: Build/load updated images in local scripts**

Ensure both setup scripts build/load the new `dbt-base` before service images and clean up `app=continuo-dbt-worker` Deployments/Secrets/pods. No worker process is added to Compose; executor creates pools only in Kubernetes.

- [ ] **Step 8: Run service and deployment tests**

```bash
rtk docker compose build executor-controller k8s-controller dbt-compile-and-load
rtk docker compose up -d executor-controller k8s-controller dbt-compile-and-load
rtk docker exec executor-controller go test ./... -count=1
rtk docker exec k8s-controller go test ./... -count=1
rtk docker exec dbt-compile-and-load python -m pytest -v tests
```

Expected: PASS with `EXECUTION_MODE=jobs` and no worker Deployment created before demand.

- [ ] **Step 9: Commit**

```bash
rtk git add executor-controller docker-compose.yml deploy/app tests/e2e/k8s scripts/setup.sh tests/e2e/provision-k8s-test-env.sh tests/e2e/cleanup-k8s-controllers.sh
rtk git commit -m "feat: deploy configurable dbt worker pools"
```

### Task 17: Prove mixed-mode behavior, performance, rollback, and documentation

**Files:**
- Create: `tests/e2e/worker_execution_test.go`
- Create: `tests/e2e/worker_failure_test.go`
- Create: `tests/e2e/worker_performance_test.go`
- Modify: `tests/e2e/helpers.go`
- Modify: `tests/e2e/system_test.go`
- Modify: `tests/e2e/README.md`
- Modify: `docs/arch/01-topology.md`
- Modify: `docs/arch/02-interaction-matrix.md`
- Modify: `docs/arch/03-sequence-flows.md`
- Modify: `docs/arch/04-service-ownership.md`
- Modify: `docs/arch/streams.md`
- Modify: `docs/arch/services/{executor-controller,k8s-controller,manifest-controller,orchestrator,release-controller}.md`

- [ ] **Step 1: Add worker e2e helpers**

Helpers query executor Postgres for deployment/lease state, Kubernetes for worker Deployments/pods/Jobs, S3 for descriptors/results, and Redis stream entries. Every waiter has a bounded timeout and reports relevant controller/worker logs on failure.

- [ ] **Step 2: Add successful execution scenarios**

`TestE2E_WorkerExecution` must prove:

```text
release compile uploads all three objects
promoted runtime objects remain after candidate cleanup and are usable by old-run rerun
promotion alone creates no worker Deployment; pending executable demand does
parallel ready service-1 branches run in separate reusable pods
sequential service-1 nodes reuse a pod
one hydration marker per pod
no app=dbt-job production Job for worker-mode tasks
service-2 Jobs still work simultaneously
active reserved slots never exceed configured test limit
view/table/incremental first+existing/seed/snapshot/test/build all match Job semantics
promotion during a run leaves its pinned old pool/ref unchanged
rerun recreates a zero-scaled old pool from its pinned image/ref
```

- [ ] **Step 3: Add failure/cancellation scenarios**

`TestE2E_WorkerFailures` covers:

```text
dbt retry uses same deployment and increments attempt
pod deletion expires/fences old token and replaces worker
late completion is rejected
schedule cancellation deletes active worker pod
idle pool scales to zero and cold-starts later
corrupt artifact fails closed
parse-context mismatch fails closed
required wrapper cache rejection never full-parses
pre-migration/missing-dbt-ID task falls back to Jobs
mode switch back to jobs is sufficient rollback
```

- [ ] **Step 4: Add a non-flaky performance gate**

Run at least 20 warm native tasks and 10 Job-path tasks on the same fixture/image/warehouse. Measure:

```text
worker: lease accepted timestamp -> first dbt invocation event
job: executor reservation timestamp -> first dbt invocation event
```

Sort durations and use nearest-rank p95:

```go
require.LessOrEqual(t, workerP95, time.Second)
require.LessOrEqual(t, float64(workerP95), float64(jobP95)*0.20)
```

Write cold worker, warm worker, wrapper, and Job samples/p50/p95 to `/tmp/continuo-worker-performance.json` and include it in failure output.

- [ ] **Step 5: Run focused e2e**

Follow `tests/e2e/README.md` setup, then:

```bash
rtk docker exec -e UI_HTTP_BASE=http://ui:8090 orchestrator go test -v -count=1 -timeout 25m -run 'TestE2E_Worker' /app/tests/e2e/...
```

Expected: all worker tests PASS.

- [ ] **Step 6: Reconcile the complete architecture pack**

Ensure the final documents state:

```text
release artifact ownership and retention
all five pinned runtime properties
pull lease API/auth/fencing
Jobs vs workers routing and rollback
one shared capacity definition
executor vs k8s-controller completion ownership
wrapper cache qualification
cancellation/crash/at-least-once semantics
pool scale-up, busy protection, idle zero-scale
executor.job.terminal:v1 and additive existing-stream fields
```

Run a text guard proving every mentioned stream exists in `contract.yaml` and no document claims one-pod-per-production-node as the only path.

- [ ] **Step 7: Run all unit/integration/lint checks in containers**

```bash
rtk docker exec executor-controller go test ./... -count=1
rtk docker exec k8s-controller go test ./... -count=1
rtk docker exec orchestrator go test ./... -count=1
rtk docker exec release-controller go test ./... -count=1
rtk docker exec manifest-controller uv run pytest -v
rtk docker exec dbt-compile-and-load python -m pytest -v tests
rtk docker exec orchestrator bash /app/scripts/lint-go.sh executor-controller
rtk docker exec orchestrator bash /app/scripts/lint-go.sh k8s-controller
rtk docker exec orchestrator bash /app/scripts/lint-go.sh orchestrator
rtk docker exec orchestrator bash /app/scripts/lint-go.sh release-controller
```

Expected: every command PASS.

- [ ] **Step 8: Run the mandatory complete e2e suite**

Read the current `tests/e2e/README.md` again, then run its complete procedure:

```bash
rtk make e2e-full
```

Expected: the full suite PASS, not only worker tests.

- [ ] **Step 9: Run generation and repository guards**

```bash
rtk docker run --rm -v /Users/simonecarolini/github/continuo:/src -w /src golang:1.25.1 sh -lc 'go generate ./pkg/streams/... && git diff --exit-code pkg/streams/streams.gen.go manifest-controller/streams_contract.py'
rtk git diff --check
rtk git status --short
```

Expected: generation produces no diff, `git diff --check` exits 0, and status contains only intended branch changes plus the pre-existing user-owned `.superpowers/` and `docs/known-issues/` entries.

- [ ] **Step 10: Commit final tests and docs**

```bash
rtk git add tests/e2e docs/arch
rtk git commit -m "test: verify reusable parse-free dbt workers"
```

- [ ] **Step 11: Verify before claiming completion**

Use `superpowers:verification-before-completion`, record the exact final test outputs, then use `superpowers:requesting-code-review`. Resolve findings with `superpowers:receiving-code-review` and rerun affected tests.

- [ ] **Step 12: Present branch integration choices**

Use `superpowers:finishing-a-development-branch`. Do not merge directly to `main`; the repository requires a PR. Do not push or create the PR until the user chooses that integration action.

---

## Delivery checkpoints

1. Tasks 1-5: artifacts and pinning shipped with `jobs` behavior unchanged.
2. Tasks 6-10: leases, shared capacity, and API shipped but no pool enabled.
3. Tasks 11-13: worker-enabled base image and wrapper qualification available.
4. Tasks 14-16: pool reconciliation available behind config.
5. Task 17: canary/e2e/performance accepted before global `workers` mode is considered.

Rollback at every checkpoint is either no behavior change (`jobs` default) or a per-service override back to `jobs`. No rollback path silently parses inside a worker.
