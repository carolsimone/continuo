"""Compares benchmark result files, or summarises one, as a Markdown table.

A stage is timed per node: its seconds over a run's dbt and python nodes, so a
size-100 run and a size-1000 run are read on one scale. The value of a stage at
a size, column count, put latency and pass is the median over the repetitions.
A stage is flagged `slower` when its value in the second file is more than the
threshold above its value in the first. The report never gates: it exits 0
whatever the numbers say.
"""
import argparse
import json
import logging
import statistics
import sys
from dataclasses import dataclass
from pathlib import Path

logger = logging.getLogger(__name__)

_HANDLER = "handler"
_PASS_ORDER = {"cold": 0, "warm": 1}
_LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s %(message)s"


@dataclass(frozen=True)
class ComparisonRow:
    """One stage at one size, column count, put latency and pass: its median
    milliseconds per node in the first file (`before`) and the second (`after`),
    the relative `change` (None when either side is missing or `before` is
    zero), and whether it is `slower` by more than the threshold."""
    size: int
    columns: int
    put_latency_ms: float
    pass_name: str
    stage: str
    before: float | None
    after: float | None
    change: float | None
    slower: bool


@dataclass(frozen=True)
class SummaryRow:
    """One stage of one file: its median milliseconds and its median
    milliseconds per node over the repetitions."""
    size: int
    columns: int
    put_latency_ms: float
    pass_name: str
    stage: str
    median_ms: float
    median_ms_per_node: float


_Key = tuple[int, int, float, str, str]


def load(path: str | Path) -> list[dict]:
    """The result lines of a JSON-lines file; blank lines are skipped."""
    with open(path) as results:
        return [json.loads(line) for line in results if line.strip()]


def _stage_seconds(line: dict) -> dict[str, float]:
    """The seconds of every stage of a result line, and of the whole handler."""
    seconds = {name: record["seconds"] for name, record in line["stages"].items()}
    seconds[_HANDLER] = line["handler_seconds"]
    return seconds


def _medians(lines: list[dict]) -> tuple[dict[_Key, tuple[float, float]], list[str]]:
    """The (median ms, median ms per node) of each key, and the stage names in
    the order they first appear."""
    milliseconds: dict[_Key, list[tuple[float, float]]] = {}
    stage_order: list[str] = []
    for line in lines:
        nodes = line["nodes"]["dbt"] + line["nodes"]["python"]
        for stage, seconds in _stage_seconds(line).items():
            if stage not in stage_order:
                stage_order.append(stage)
            key = (line["size"], line["columns"], line["put_latency_ms"], line["pass"], stage)
            milliseconds.setdefault(key, []).append((seconds * 1000, seconds * 1000 / nodes))
    medians = {
        key: (
            statistics.median(total for total, _ in values),
            statistics.median(per_node for _, per_node in values),
        )
        for key, values in milliseconds.items()
    }
    # The handler's own total is the last row of each group.
    if _HANDLER in stage_order:
        stage_order.remove(_HANDLER)
        stage_order.append(_HANDLER)
    return medians, stage_order


def _sorted_keys(keys: set[_Key], stage_order: list[str]) -> list[_Key]:
    return sorted(keys, key=lambda key: (
        key[0], key[1], key[2], _PASS_ORDER.get(key[3], len(_PASS_ORDER)), stage_order.index(key[4]),
    ))


def summarise(lines: list[dict]) -> list[SummaryRow]:
    """Each stage's median milliseconds and milliseconds per node."""
    medians, stage_order = _medians(lines)
    return [
        SummaryRow(*key[:3], key[3], key[4], *medians[key])
        for key in _sorted_keys(set(medians), stage_order)
    ]


def compare(before: list[dict], after: list[dict], *, threshold: float = 0.20) -> list[ComparisonRow]:
    """Each stage's median milliseconds per node in `before` and in `after`. A
    row is `slower` when `after` is above `before * (1 + threshold)`; a stage
    only one side has is listed and is never slower."""
    before_medians, before_order = _medians(before)
    after_medians, after_order = _medians(after)
    stage_order = before_order + [stage for stage in after_order if stage not in before_order]
    rows = []
    for key in _sorted_keys(set(before_medians) | set(after_medians), stage_order):
        old = before_medians[key][1] if key in before_medians else None
        new = after_medians[key][1] if key in after_medians else None
        both = old is not None and new is not None
        rows.append(ComparisonRow(
            size=key[0], columns=key[1], put_latency_ms=key[2], pass_name=key[3], stage=key[4],
            before=old, after=new,
            change=(new / old - 1) if both and old else None,
            slower=both and new > old * (1 + threshold),
        ))
    return rows


def _number(value: float | None) -> str:
    return "" if value is None else f"{value:.4f}"


def _summary_table(rows: list[SummaryRow]) -> str:
    lines = [
        "| size | columns | put ms | pass | stage | median ms | median ms per node |",
        "|---:|---:|---:|---|---|---:|---:|",
    ]
    lines.extend(
        f"| {row.size} | {row.columns} | {row.put_latency_ms:g} | {row.pass_name} | {row.stage} "
        f"| {row.median_ms:.1f} | {_number(row.median_ms_per_node)} |"
        for row in rows
    )
    return "\n".join(lines) + "\n"


def _comparison_table(rows: list[ComparisonRow]) -> str:
    lines = [
        "| size | columns | put ms | pass | stage | before ms per node | after ms per node | change | |",
        "|---:|---:|---:|---|---|---:|---:|---:|---|",
    ]
    lines.extend(
        f"| {row.size} | {row.columns} | {row.put_latency_ms:g} | {row.pass_name} | {row.stage} "
        f"| {_number(row.before)} | {_number(row.after)} "
        f"| {'' if row.change is None else f'{row.change:+.1%}'} | {'slower' if row.slower else ''} |"
        for row in rows
    )
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m bench.compare",
        description="Summarise one result file, or compare two, as a Markdown table. Exits 0 whatever the numbers say.",
    )
    parser.add_argument("before", help="the result file to summarise, or the baseline of a comparison")
    parser.add_argument("after", nargs="?", help="the result file to compare with the first")
    parser.add_argument("--out", required=True, help="the Markdown file to write")
    parser.add_argument("--threshold", type=float, default=0.20,
                        help="the relative rise in median ms per node above which a stage is slower")
    args = parser.parse_args(argv)
    logging.basicConfig(stream=sys.stderr, level=logging.INFO, format=_LOG_FORMAT)

    try:
        before = load(args.before)
        after = load(args.after) if args.after else None
    except (OSError, json.JSONDecodeError) as exc:
        logger.error("cannot read the result files: %s", exc)
        return 2

    if after is None:
        table = _summary_table(summarise(before))
        logger.info("summarised %s", args.before)
    else:
        rows = compare(before, after, threshold=args.threshold)
        table = _comparison_table(rows)
        slower = sum(row.slower for row in rows)
        logger.info("%d of %d stages are slower by more than %.0f%%", slower, len(rows), args.threshold * 100)
    Path(args.out).write_text(table)
    return 0


if __name__ == "__main__":
    sys.exit(main())
