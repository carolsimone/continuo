# Release parse benchmark

This package times the release parse of topology-controller on synthetic
estates. It drives the real `CandidateManifestHandler`, with its real parsers,
resolver, rewriter, builders and writers, over an in-memory S3, and reads the
per-stage record the handler keeps (`service/release_timings.py`). It uses the
standard library and the service's own dependencies, so it adds nothing to
`pyproject.toml`.

It runs by hand. No workflow, make target or test runs it, and its results never
gate anything: a regression shows in a table someone reads.

## Run it on the host

```bash
cd topology-controller
uv run python -m bench.run --sizes 100,1000,10000 --reps 3 --out bench/results/<name>.jsonl
```

`bench/results/` is git-ignored and docker-ignored.

## Run it on one CPU

The chart gives the pod one CPU. The dev image under `--cpus=1` matches it:

```bash
mkdir -p topology-controller/bench/results
docker build -t tc-bench:local -f topology-controller/Dockerfile.dev topology-controller
docker run --rm --cpus=1 -v "$PWD/topology-controller/bench/results:/app/bench/results" \
  tc-bench:local uv run python -m bench.run --sizes 100,1000,10000 --reps 3 --out bench/results/<name>.jsonl
```

The dev image has no `git` and no `.git`, so `commit` and `dirty` read `null`
there.

## Flags

| Flag | Default | Meaning |
|---|---|---|
| `--sizes` | required | Comma-separated model counts, for example `100,1000,10000`. |
| `--reps` | `3` | Repetitions of each size. Each repetition runs in a new process. |
| `--columns` | `20` | Columns of every model, and of every python node's output. At least 3. |
| `--put-latency-ms` | `0.0` | Milliseconds every S3 put and download sleeps, standing in for a network round trip. |
| `--dialect` | `postgres` | The warehouse's SQL dialect, `postgres` or `trino`. |
| `--out` | required | The JSON-lines file to write. |

`--child`, `--size` and `--rep` are internal: the parent starts one child per
size and repetition.

## What a run does

For each size and repetition the parent starts a child process. The child builds
the estate, then parses the release twice with the same estate and one in-memory
S3:

- the `cold` pass is the first parse of a new process, so nothing in it is warm;
- the `warm` pass parses the same release again in the same process.

The child's own log lines go to its stderr, formatted as `main.py` formats them,
so the handler pays the logging cost it pays in production. The parent drops a
successful child's stderr. When a child fails, the parent logs the last 50 lines
of that child's stderr and exits 1.

## The estate

`bench/estate.py` builds the S3 objects of one release from `random.Random(seed)`
(seed 1 unless a caller passes another), so the same arguments give the same
bytes and every baseline parses the same estate.

- 10 dbt services, `svc-0` to `svc-9`, with the models split evenly, and one
  python service, `svc-py`, of one node per ten models.
- Each model has `--columns` columns, a compiled SQL of a CTE over one upstream,
  a window sum and a left join to a second, and two generic not-null tests.
- Each dbt service has one seed, and a `compile-extras.json` beside its manifest.
- Each manifest carries the macros of `fixtures/macros.json.gz`, which
  `dbt parse` of the finance service of continuo-demo (commit `3ab3a7e`) wrote
  with dbt-core 1.12.0b1 and dbt-postgres 1.10.0, the versions continuo's dbt
  images pin. The fixture stays the same from one baseline to the next.

## The result line

One JSON object per line, one line per size, pass and repetition.

| Field | Meaning |
|---|---|
| `schema` | The version of this field list, `1`. |
| `commit` | `git rev-parse HEAD` of the checkout, or `null` without git. |
| `dirty` | Whether `git status --porcelain` lists changes, or `null` without git. |
| `python` | The interpreter's version. |
| `packages` | The installed versions of `sqlglot`, `PyYAML`, `jsonschema`, `continuo-contracts` and `prometheus-client`; a package that is not installed is absent. |
| `libyaml` | Whether PyYAML was built with libyaml. |
| `cpu` | The CPU's name from `sysctl`, else the `model name` line of `/proc/cpuinfo`, else `null`. |
| `cpu_limit` | The cgroup CPU quota in CPUs (`/sys/fs/cgroup/cpu.max`), or `null`. |
| `dialect` | The SQL dialect the handler ran with. |
| `size` | The model count. |
| `columns` | The columns of each model. |
| `put_latency_ms` | The latency of each S3 put and download. |
| `pass` | `cold` or `warm`. |
| `rep` | The repetition number, from 1. |
| `nodes` | `{"dbt": n, "python": n}`: the dbt models, tests and seeds, and the python nodes. |
| `puts` | The S3 puts of the pass. |
| `put_bytes` | The bytes of those puts. |
| `handler_seconds` | The wall time of `handle()`. |
| `stages` | One entry per stage, `{"seconds": s, "runs": n, "counts": {...}}`: the seconds summed over its runs, how many times it ran, and what it handled. A stage that did not run reads zero. |
| `peak_rss_bytes` | The largest resident set the handler read at a stage boundary. It includes the estate held in the process, and is a lower bound of the true peak. |
| `max_rss_bytes` | The process's peak resident set from `getrusage`, over every pass so far. |

## Compare two runs

```bash
cd topology-controller
uv run python -m bench.compare bench/results/before.jsonl bench/results/after.jsonl --out bench/results/compare.md
```

The table holds one row per size, column count, put latency, pass and stage,
with every stage of the handler and a `handler` row for the whole parse. A
stage's value is the median over the repetitions of its milliseconds per node
(its seconds over `nodes.dbt` plus `nodes.python`), so sizes read on one scale.
A stage whose value rose by more than 20 percent (`--threshold 0.20`) is marked
`slower`. With one file, `python -m bench.compare <file> --out <md>` prints the
median milliseconds and milliseconds per node of each stage.

The report logs how many stages are slower and exits 0 whatever the numbers say.
It exits 2 when a file cannot be read.

## The rules

- Nothing else runs in the colima VM during a measurement: the benchmark shares
  its CPU, and any other load skews the numbers.
- Results never gate. Nothing in CI reads them.
- A named baseline goes to `carolsimone/arch` under
  `continuo/design/<date>-topology-controller-baseline/`, with the raw JSON lines
  and a report.
