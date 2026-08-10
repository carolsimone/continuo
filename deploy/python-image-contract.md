# Python image contract

What a domain team's python image must provide to run under Continuo.
executor-controller launches your image as a Kubernetes Job for scheduled runs
of your `python-model` nodes; this page is Continuo's side of that contract.

The normative specification of what the image must *do* with the environment
below — the harness, the baked contract files, output conformance, the result
block — lives in
[`continuo-python-runtime`](https://github.com/carolsimone/continuo-python-runtime)
under `docs/boundary-contract.md`. Where the two disagree, that document wins.

For dbt nodes, whose images are resolved and configured differently, see
[`dbt-image-contract.md`](dbt-image-contract.md).

## Image resolution

Unlike dbt images, a python node's image is **not** composed from the service
name. The `image_tag` you send on `POST /releases` is a complete registry
reference (`<registry>/<image>:<tag>`), and the executor runs it verbatim: no
prefix is prepended, and the service name is not used. `global.teamImagePrefix`
does not apply.

The reference must carry an explicit tag or digest. An untagged reference is
refused permanently, because `:latest` would make the code that actually ran
unidentifiable from the release that promoted it.

Build your image `FROM` the engine-matched runtime base
(`ghcr.io/carolsimone/continuo-python-runtime:<version>-<engine>`), which sets
the entrypoint, the contract directory, and the import root. One image serves
all of the service's nodes — the harness selects the right one at run time.

## What your container must do

The executor sets **no command**: your image's entrypoint is the runtime
harness, which looks up the node named by `NODE_ID` in the contract files baked
into the image, imports its `script`, runs it, conforms the returned dataframe
to the declared columns, and performs the only write. Overriding the entrypoint
bypasses that sole write sink.

## Environment your container receives

| Variable | Meaning |
|---|---|
| `NODE_ID` | The node's identity, `<schema>.<table>`. The harness matches its trailing two dot-separated segments against the contract files in your image. |
| `TABLE_NAME` | The target table. Must match the selected node's `table`. |
| `TARGET_SCHEMA` | The target schema. Must match the selected node's `schema`. There is **no fallback** — `SCHEMA` and `DBT_TARGET_SCHEMA` are dbt names and are deliberately not set. |

Plus the warehouse connection: the operator-owned warehouse Secret is attached
to your container verbatim via `envFrom`, exactly as for dbt images, so the
runtime adapter baked into your image reads the deployment engine's native keys
(`POSTGRES_HOST` / `POSTGRES_DB` / `POSTGRES_USER` / …, or that engine's
equivalents). A missing one fails the node with a `LoadError`.

`CONTRACT_DIR` and `APP_ROOT` are **not** set by the executor — the runtime base
image declares them, because the image owns its own layout.

Nothing else is injected. In particular your image never receives S3
credentials, and the pod has no init containers and no mounted volumes: your
contract files and scripts travel inside the image itself.

## Which operations reach your image

Only `run` and `build`, and both dispatch identically. A python node's contract
declares no tests, so `build` — materialize and test in one step — reduces to
the same work as `run`, exactly as `dbt build` does on a model with no tests. A
`test` run skips python nodes entirely and never starts a pod.

## What Continuo does with your output

Your container's stdout must end with exactly one sentinel-framed result block
(the harness emits it); every other diagnostic goes to stderr. On a terminal
Job, Continuo strips that block from the text log, uploads the log and the
block's JSON to S3 — on success as well as failure — and records both keys on
the run's execution row. A non-zero exit fails the node, and the block's error
class (`ContractError`, `ReadError`, `ScriptError`, `ConformError`, `LoadError`)
is what downstream remediation keys off.
